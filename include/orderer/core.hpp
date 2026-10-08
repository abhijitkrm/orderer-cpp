// core.hpp — the MatchingCore seam: what a partition's engine thread needs
// from a matching core (orderer-rust's orderer-core/src/core.rs).
#pragma once

#include <algorithm>
#include <concepts>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <matcher/matcher.hpp>

#include "flat.hpp"

namespace orderer {

using namespace matcher;
using RestingOrder = OrderBook::RestingOrder;
using Block = std::pair<Symbol, std::string>;

/// A core is constructed from the default book config, applies commands
/// emitting (symbol, seq, event), contributes matcher-snap/1 book blocks,
/// and restores a book from a snapshot block (error text on failure).
template <class C>
concept MatchingCore = requires(C c, Symbol s, const Command& cmd, std::vector<Block>& blocks,
                                const std::vector<RestingOrder>& orders) {
    C(BookConfig{});
    c.apply(s, cmd, [](Symbol, std::uint64_t, const Event&) {});
    c.snapshot_blocks(blocks);
    { c.restore_book(s, std::uint64_t{}, orders) } -> std::same_as<std::optional<std::string>>;
};

/// Can `orders` be restored as `sym`'s book under `cfg`? (orderer-rust
/// snapshot::validate_book: capacity, unique ids, qty > 0, prices, uncrossed.)
inline std::optional<std::string> validate_book(const BookConfig& cfg, Symbol sym,
                                                const std::vector<RestingOrder>& orders) {
    auto err = [&](const std::string& m) {
        return std::optional<std::string>("snapshot book " + std::to_string(sym) + ": " + m);
    };
    if (orders.size() > cfg.max_orders)
        return err(std::to_string(orders.size()) + " orders exceed max_orders " +
                   std::to_string(cfg.max_orders));
    std::vector<OrderId> ids;
    for (const auto& o : orders) ids.push_back(o.order_id);
    std::sort(ids.begin(), ids.end());
    if (std::adjacent_find(ids.begin(), ids.end()) != ids.end()) return err("duplicate order_id");
    bool have_bid = false, have_ask = false;
    Price best_bid = 0, best_ask = 0;
    for (const auto& o : orders) {
        if (o.qty == 0) return err("order " + std::to_string(o.order_id) + " has qty 0");
        bool ok = cfg.index == IndexKind::Ladder ? (o.price >= cfg.price_min && o.price <= cfg.price_max)
                                                 : o.price > 0;
        if (!ok) return err("order " + std::to_string(o.order_id) + " price out of range");
        if (o.side == Side::Bid) {
            best_bid = have_bid ? std::max(best_bid, o.price) : o.price;
            have_bid = true;
        } else {
            best_ask = have_ask ? std::min(best_ask, o.price) : o.price;
            have_ask = true;
        }
    }
    if (have_bid && have_ask && best_bid >= best_ask) return err("crossed book");
    return std::nullopt;
}

/// The spec-proven FIFO core: matcher's Engine, unchanged.
class FifoCore {
  public:
    explicit FifoCore(BookConfig cfg) : eng_(cfg) {}

    template <class F>
    void apply(Symbol sym, const Command& cmd, F&& emit) {
        eng_.submit_tagged(sym, cmd, emit);
    }

    void snapshot_blocks(std::vector<Block>& out) {
        for (Symbol s : eng_.symbols()) {
            std::string b;
            snapshot::write_book(*eng_.book(s), s, b);
            out.emplace_back(s, std::move(b));
        }
    }

    std::optional<std::string> restore_book(Symbol sym, std::uint64_t seq,
                                            const std::vector<RestingOrder>& orders) {
        BookConfig cfg = eng_.default_config();
        if (auto e = validate_book(cfg, sym, orders)) return e;
        eng_.add_book(sym, OrderBook::restore(cfg, seq, orders));
        return std::nullopt;
    }

    Engine& engine() { return eng_; }

  private:
    Engine eng_;
};

/// Test core: echoes each command as one event with a dense per-symbol seq
/// — new → accepted{qty}, cancel → closed{cancelled}, replace → replaced.
class NoopCore {
  public:
    explicit NoopCore(BookConfig) {}

    template <class F>
    void apply(Symbol sym, const Command& cmd, F&& emit) {
        std::uint64_t& seq = seqs_[sym];
        ++seq;
        Event ev{};
        ev.order_id = cmd.order_id;
        switch (cmd.kind) {
            case Command::Kind::New:
                ev.kind = Event::Kind::Accepted;
                ev.leaves_qty = cmd.qty;
                break;
            case Command::Kind::Cancel:
                ev.kind = Event::Kind::Closed;
                ev.reason = std::uint8_t(CloseReason::Cancelled);
                break;
            case Command::Kind::Replace:
                ev.kind = Event::Kind::Replaced;
                ev.price = cmd.price;
                ev.qty = cmd.qty;
                break;
        }
        emit(sym, seq, ev);
    }

    void snapshot_blocks(std::vector<Block>& out) {
        for (auto& [s, q] : seqs_)
            out.emplace_back(s, "{\"rec\":\"book\",\"symbol\":" + std::to_string(s) +
                                    ",\"seq\":" + std::to_string(q) + "}\n");
    }

    std::optional<std::string> restore_book(Symbol sym, std::uint64_t seq,
                                            const std::vector<RestingOrder>&) {
        seqs_[sym] = seq;
        return std::nullopt;
    }

  private:
    std::unordered_map<Symbol, std::uint64_t> seqs_;
};

// ---- matcher-snap/1, strictly ------------------------------------------------------

struct SnapBook {
    Symbol symbol = 0;
    std::uint64_t seq = 0;
    std::vector<RestingOrder> orders;
};

struct ParsedSnapshot {
    BookConfig cfg;
    std::vector<SnapBook> books;
};

/// The header line for a default book config.
inline void write_snapshot_header(const BookConfig& c, std::string& out) {
    out += "{\"format\":\"matcher-snap/1\",\"pmin\":";
    append_i(out, c.price_min);
    out += ",\"pmax\":";
    append_i(out, c.price_max);
    out += ",\"max_orders\":";
    append_u(out, c.max_orders);
    out += ",\"index\":\"";
    out += flat::index_name(c.index);
    out += "\"}\n";
}

/// Parse a snapshot, reporting malformed input (orderer-rust try_parse).
inline std::optional<std::string> parse_snapshot(std::string_view text, ParsedSnapshot& out) {
    using flat::get_i64, flat::get_str, flat::get_u64;
    auto err = [](std::size_t n, const std::string& m) {
        return std::optional<std::string>("snapshot line " + std::to_string(n) + ": " + m);
    };
    std::size_t pos = 0, n = 0;
    auto next_line = [&](std::string_view& line) {
        if (pos > text.size() || (pos == text.size())) return false;
        auto e = text.find('\n', pos);
        if (e == std::string_view::npos) e = text.size();
        line = text.substr(pos, e - pos);
        pos = e + 1;
        ++n;
        return true;
    };
    std::string_view hdr;
    if (!next_line(hdr)) return err(1, "empty snapshot");
    if (get_str(hdr, "format") != std::optional<std::string_view>("matcher-snap/1"))
        return err(1, "not a matcher-snap/1 header");
    out.cfg = flat::parse_header(hdr);
    out.books.clear();
    std::string_view line;
    while (next_line(line)) {
        if (line.empty()) continue;
        auto rec = get_str(line, "rec");
        if (rec && *rec == "book") {
            SnapBook b;
            b.symbol = Symbol(get_u64(line, "symbol").value_or(0));
            b.seq = get_u64(line, "seq").value_or(0);
            out.books.push_back(std::move(b));
        } else if (rec && *rec == "order") {
            RestingOrder o{};
            auto id = get_u64(line, "order_id");
            auto side = get_str(line, "side");
            auto tif = get_str(line, "tif");
            auto price = get_i64(line, "price");
            auto qty = get_u64(line, "qty");
            if (!id) return err(n, "bad order_id");
            if (!side || (*side != "bid" && *side != "ask")) return err(n, "bad side");
            o.side = *side == "ask" ? Side::Ask : Side::Bid;
            if (!tif) return err(n, "bad tif");
            if (*tif == "gtc") o.tif = Tif::Gtc;
            else if (*tif == "ioc") o.tif = Tif::Ioc;
            else if (*tif == "fok") o.tif = Tif::Fok;
            else if (*tif == "post_only") o.tif = Tif::PostOnly;
            else return err(n, "bad tif");
            if (!price) return err(n, "bad price");
            if (!qty) return err(n, "bad qty");
            o.order_id = *id;
            o.price = *price;
            o.qty = *qty;
            if (out.books.empty()) return err(n, "order line before book block");
            out.books.back().orders.push_back(o);
        } else {
            return err(n, "bad rec");
        }
    }
    return std::nullopt;
}

}  // namespace orderer
