// OrderBook — single-writer deterministic FIFO price-time book.
// Mirrors matcher-rust/src/book.rs.
#pragma once

#include <limits>

#include "detail/internals.hpp"
#include "types.hpp"

namespace matcher {

struct OrderInfo {
    OrderId order_id;
    Side side;
    Price price;
    Qty qty;
};

class OrderBook {
    using Level = detail::Level;
    using Order = detail::Order;

  public:
    explicit OrderBook(BookConfig cfg)
        : pool_(cfg.max_orders), map_(cfg.max_orders),
          bids_(cfg.index, Side::Bid, cfg.price_min, cfg.price_max),
          asks_(cfg.index, Side::Ask, cfg.price_min, cfg.price_max), cfg_(cfg) {}

    /// Apply one command; events emitted in order via sink.on_event(seq, ev).
    template <class Sink>
    void apply(const Command& cmd, Sink& sink) {
        switch (cmd.kind) {
            case Command::Kind::New:
                new_order(cmd, sink);
                break;
            case Command::Kind::Cancel:
                cancel(cmd.order_id, sink);
                break;
            case Command::Kind::Replace:
                replace(cmd.order_id, cmd.price, cmd.qty, sink);
                break;
        }
    }

    // ---- commands ----

    template <class Sink>
    void new_order(const Command& c, Sink& sink) {
        const OrderId oid = c.order_id;
        // SPEC §4.3 validation precedence.
        if (c.qty == 0) return reject(sink, oid, RejectReason::InvalidQty);
        if (c.otype == OType::Limit && !price_ok(c.price))
            return reject(sink, oid, RejectReason::InvalidPrice);
        if (map_.contains(oid))
            return reject(sink, oid, RejectReason::DuplicateOrderId);
        if (pool_.live >= cfg_.max_orders)
            return reject(sink, oid, RejectReason::BookFull);
        if (c.otype == OType::Limit) {
            if (c.tif == Tif::PostOnly && would_cross(c.side, c.price))
                return reject(sink, oid, RejectReason::PostOnlyWouldCross);
            if (c.tif == Tif::Fok && fillable(c.side, c.price) < c.qty)
                return reject(sink, oid, RejectReason::FokCannotFill);
        }

        Qty remaining = c.qty;
        const bool bound = c.otype == OType::Limit;
        cross(c.side, bound, c.price, oid, remaining, sink);

        if (remaining == 0) {
            Event e; e.kind = Event::Kind::Closed; e.order_id = oid;
            e.reason = std::uint8_t(CloseReason::Filled);
            emit(sink, e);
        } else if (c.otype == OType::Limit && (c.tif == Tif::Gtc || c.tif == Tif::PostOnly)) {
            rest(oid, c.side, c.price, remaining, c.tif);
            Event e; e.kind = Event::Kind::Accepted; e.order_id = oid; e.leaves_qty = remaining;
            emit(sink, e);
        } else {
            Event e; e.kind = Event::Kind::Closed; e.order_id = oid;
            e.reason = std::uint8_t(CloseReason::Expired);
            emit(sink, e);
        }
    }

    template <class Sink>
    void cancel(OrderId oid, Sink& sink) {
        const std::uint32_t* pidx = map_.get(oid);
        if (!pidx) return reject(sink, oid, RejectReason::UnknownOrderId);
        const std::uint32_t idx = *pidx;
        const Order& o = pool_.slots[idx];
        auto& own = own_index(o.side);
        if (Level* lv = own.level_mut(o.price)) detail::level_unlink(pool_, *lv, idx);
        own.unlink_level(o.price);
        map_.remove(oid);
        pool_.free(idx);
        Event e; e.kind = Event::Kind::Closed; e.order_id = oid;
        e.reason = std::uint8_t(CloseReason::Cancelled);
        emit(sink, e);
    }

    template <class Sink>
    void replace(OrderId oid, Price price, Qty qty, Sink& sink) {
        const std::uint32_t* pidx = map_.get(oid);
        if (!pidx) return reject(sink, oid, RejectReason::UnknownOrderId);
        const std::uint32_t idx = *pidx;
        if (qty == 0) return reject(sink, oid, RejectReason::InvalidQty);
        if (!price_ok(price)) return reject(sink, oid, RejectReason::InvalidPrice);
        Order& o = pool_.slots[idx];
        const Price old_price = o.price;
        const Qty old_qty = o.qty;
        const Side side = o.side;

        if (price == old_price && qty <= old_qty) {
            auto& own = own_index(side);
            if (Level* lv = own.level_mut(price)) lv->total -= old_qty - qty;
            o.qty = qty;
            Event e; e.kind = Event::Kind::Replaced; e.order_id = oid; e.price = price; e.qty = qty;
            emit(sink, e);
            return;
        }

        auto& own = own_index(side);
        if (Level* lv = own.level_mut(old_price)) detail::level_unlink(pool_, *lv, idx);
        own.unlink_level(old_price);
        o.price = price;
        o.qty = qty;

        Qty remaining = qty;
        cross(side, true, price, oid, remaining, sink);

        if (remaining == 0) {
            map_.remove(oid);
            pool_.free(idx);
            Event e; e.kind = Event::Kind::Closed; e.order_id = oid;
            e.reason = std::uint8_t(CloseReason::Filled);
            emit(sink, e);
            return;
        }
        o.qty = remaining;
        detail::level_push(pool_, *own.level_insert(price), idx);
        Event e; e.kind = Event::Kind::Replaced; e.order_id = oid; e.price = price;
        e.qty = remaining;
        emit(sink, e);
    }

    // ---- matching core ----

    template <class Sink>
    void cross(Side side, bool has_bound, Price bound, OrderId taker, Qty& qty, Sink& sink) {
        detail::PriceIndex& opp = side == Side::Bid ? asks_ : bids_;
        for (;;) {
            Price bp;
            if (!opp.best_price(bp)) return;
            if (has_bound) {
                if (side == Side::Bid && bp > bound) return;
                if (side == Side::Ask && bp < bound) return;
            }
            Level* lv = opp.level_mut(bp);
            if (!lv) return;
            for (;;) {
                const std::uint32_t mi = lv->head;
                if (mi == detail::NIL) break;
                Order& m = pool_.slots[mi];
                const OrderId mid = m.id;
                const Qty mqty = m.qty;
                const Qty q = qty < mqty ? qty : mqty;
                ++seq_;
                {
                    Event e; e.kind = Event::Kind::Trade; e.maker = mid; e.taker = taker;
                    e.price = bp; e.qty = q;
                    sink.on_event(seq_, e);
                }
                lv->total -= q;
                m.qty = mqty - q;
                qty -= q;
                if (mqty == q) {
                    detail::level_unlink(pool_, *lv, mi);
                    map_.remove(mid);
                    pool_.free(mi);
                    ++seq_;
                    Event e; e.kind = Event::Kind::Closed; e.order_id = mid;
                    e.reason = std::uint8_t(CloseReason::Filled);
                    sink.on_event(seq_, e);
                }
                if (qty == 0) break;
            }
            if (lv->empty()) opp.unlink_level(bp);
            if (qty == 0) return;
        }
    }

    // ---- query API ----

    bool best_bid(Price& out) const { return bids_.best_price(out); }
    bool best_ask(Price& out) const { return asks_.best_price(out); }
    std::size_t order_count() const { return pool_.live; }
    std::uint64_t seq() const { return seq_; }
    std::size_t level_count(Side s) { return own_index(s).len(); }
    std::vector<detail::LevelDepth> depth(Side s, std::size_t n) {
        return own_index(s).depth(n);
    }
    bool order(OrderId oid, OrderInfo& out) const {
        const std::uint32_t* pidx = map_.get(oid);
        if (!pidx) return false;
        const Order& o = pool_.slots[*pidx];
        out = OrderInfo{o.id, o.side, o.price, o.qty};
        return true;
    }

    /// One live order, for snapshot serialization (spec/JOURNAL.md).
    struct RestingOrder {
        OrderId order_id;
        Side side;
        Price price;
        Qty qty;
        Tif tif;
    };

    /// All live orders in book order: bids best→worst then asks best→worst,
    /// FIFO within each level.
    std::vector<RestingOrder> resting_orders() {
        std::vector<RestingOrder> out;
        out.reserve(pool_.live);
        for (Side s : {Side::Bid, Side::Ask}) {
            auto& idx = own_index(s);
            for (const auto& d : idx.depth(std::numeric_limits<std::size_t>::max())) {
                Level* lv = idx.level_mut(d.price);
                for (std::uint32_t i = lv->head; i != detail::NIL; i = pool_.slots[i].next) {
                    const Order& o = pool_.slots[i];
                    out.push_back(RestingOrder{o.id, o.side, o.price, o.qty, o.tif});
                }
            }
        }
        return out;
    }

    /// Rebuild a book from a snapshot: same config, explicit seq, resting
    /// orders replayed in snapshot order (bids then asks, FIFO per level).
    static OrderBook restore(BookConfig cfg, std::uint64_t seq,
                             const std::vector<RestingOrder>& orders) {
        OrderBook b(cfg);
        b.seq_ = seq;
        for (const auto& o : orders) {
            const std::uint32_t idx = b.pool_.alloc();
            if (idx == detail::NIL) break;
            b.pool_.slots[idx] =
                Order{o.order_id, o.side, o.price, o.qty, o.tif, detail::NIL, detail::NIL};
            detail::level_push(b.pool_, *b.own_index(o.side).level_insert(o.price), idx);
            b.map_.insert(o.order_id, idx);
        }
        return b;
    }

    const BookConfig& config() const { return cfg_; }

  private:
    detail::PriceIndex& own_index(Side s) { return s == Side::Bid ? bids_ : asks_; }
    const detail::PriceIndex& own_index(Side s) const {
        return s == Side::Bid ? bids_ : asks_;
    }

    bool price_ok(Price p) const {
        if (cfg_.index == IndexKind::Tree) return p > 0;
        return p >= cfg_.price_min && p <= cfg_.price_max;
    }

    bool would_cross(Side side, Price price) const {
        Price bp;
        if (side == Side::Bid) return asks_.best_price(bp) && price >= bp;
        return bids_.best_price(bp) && price <= bp;
    }

    Qty fillable(Side side, Price price) const {
        const Price lo = cfg_.index == IndexKind::Tree ? INT64_MIN / 2 : cfg_.price_min;
        const Price hi = cfg_.index == IndexKind::Tree ? INT64_MAX / 2 : cfg_.price_max;
        return side == Side::Bid ? asks_.sum_range(lo, price) : bids_.sum_range(price, hi);
    }

    void rest(OrderId oid, Side side, Price price, Qty qty, Tif tif) {
        const std::uint32_t idx = pool_.alloc();
        if (idx == detail::NIL) return; // unreachable: book_full checked at ingest
        pool_.slots[idx] = Order{oid, side, price, qty, tif, detail::NIL, detail::NIL};
        detail::level_push(pool_, *own_index(side).level_insert(price), idx);
        map_.insert(oid, idx);
    }

    template <class Sink>
    void emit(Sink& sink, const Event& e) {
        ++seq_;
        sink.on_event(seq_, e);
    }
    template <class Sink>
    void reject(Sink& sink, OrderId oid, RejectReason r) {
        Event e; e.kind = Event::Kind::Rejected; e.order_id = oid;
        e.reason = std::uint8_t(r);
        emit(sink, e);
    }

    detail::Pool pool_;
    detail::OrderMap map_;
    detail::PriceIndex bids_, asks_;
    std::uint64_t seq_ = 0;
    BookConfig cfg_;
};

} // namespace matcher
