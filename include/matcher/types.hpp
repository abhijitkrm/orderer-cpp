// matcher — deterministic FIFO limit order book & matching engine core.
// Types mirror ../spec/SPEC.md and matcher-rust/src/types.rs.
#pragma once

#include <cstdint>
#include <string>

namespace matcher {

using OrderId = std::uint64_t;
using Symbol = std::uint32_t;
using Price = std::int64_t;
using Qty = std::uint64_t;

enum class Side : std::uint8_t { Bid, Ask };
inline Side opposite(Side s) { return s == Side::Bid ? Side::Ask : Side::Bid; }
inline const char* to_str(Side s) { return s == Side::Bid ? "bid" : "ask"; }

enum class OType : std::uint8_t { Limit, Market };
inline const char* to_str(OType t) { return t == OType::Limit ? "limit" : "market"; }

enum class Tif : std::uint8_t { Gtc, Ioc, Fok, PostOnly };
inline const char* to_str(Tif t) {
    switch (t) {
        case Tif::Gtc: return "gtc";
        case Tif::Ioc: return "ioc";
        case Tif::Fok: return "fok";
        default: return "post_only";
    }
}

struct Command {
    enum class Kind : std::uint8_t { New, Cancel, Replace } kind;
    OrderId order_id = 0;
    Side side = Side::Bid;
    OType otype = OType::Limit;
    Price price = 0;
    Qty qty = 0;
    Tif tif = Tif::Gtc;

    static Command new_limit(OrderId id, Side s, Price p, Qty q, Tif t) {
        Command c; c.kind = Kind::New; c.order_id = id; c.side = s;
        c.otype = OType::Limit; c.price = p; c.qty = q; c.tif = t; return c;
    }
    static Command new_market(OrderId id, Side s, Qty q) {
        Command c; c.kind = Kind::New; c.order_id = id; c.side = s;
        c.otype = OType::Market; c.qty = q; c.tif = Tif::Ioc; return c;
    }
    static Command cancel(OrderId id) {
        Command c; c.kind = Kind::Cancel; c.order_id = id; return c;
    }
    static Command replace(OrderId id, Price p, Qty q) {
        Command c; c.kind = Kind::Replace; c.order_id = id; c.price = p; c.qty = q; return c;
    }
};

enum class RejectReason : std::uint8_t {
    InvalidQty, InvalidPrice, DuplicateOrderId, UnknownOrderId,
    PostOnlyWouldCross, FokCannotFill, BookFull,
};
inline const char* to_str(RejectReason r) {
    switch (r) {
        case RejectReason::InvalidQty: return "invalid_qty";
        case RejectReason::InvalidPrice: return "invalid_price";
        case RejectReason::DuplicateOrderId: return "duplicate_order_id";
        case RejectReason::UnknownOrderId: return "unknown_order_id";
        case RejectReason::PostOnlyWouldCross: return "post_only_would_cross";
        case RejectReason::FokCannotFill: return "fok_cannot_fill";
        default: return "book_full";
    }
}

enum class CloseReason : std::uint8_t { Filled, Cancelled, Expired };
inline const char* to_str(CloseReason r) {
    switch (r) {
        case CloseReason::Filled: return "filled";
        case CloseReason::Cancelled: return "cancelled";
        default: return "expired";
    }
}

// Flattened event record (Kind selects fields — same as Go port).
struct Event {
    enum class Kind : std::uint8_t { Accepted, Rejected, Trade, Closed, Replaced } kind;
    OrderId order_id = 0;
    OrderId maker = 0, taker = 0;
    Price price = 0;
    Qty qty = 0;
    Qty leaves_qty = 0;
    std::uint8_t reason = 0;

    // Cheap content hash so sinks observe every field (benchmarks).
    std::uint64_t fold() const {
        switch (kind) {
            case Kind::Trade:
                return maker * 0x9E3779B3u + taker * 0x85EBCA6Bu +
                       std::uint64_t(price) * 0xC2B2AE35u + qty;
            default:
                return order_id * 0x9E3779B1u + std::uint64_t(kind) + qty + leaves_qty + reason;
        }
    }
};

inline void append_u(std::string& s, std::uint64_t v) {
    char tmp[20];
    int i = 20;
    do { tmp[--i] = char('0' + v % 10); v /= 10; } while (v);
    s.append(tmp + i, 20 - i);
}
inline void append_i(std::string& s, std::int64_t v) {
    if (v < 0) { s.push_back('-'); append_u(s, std::uint64_t(-(v + 1)) + 1); return; }
    append_u(s, std::uint64_t(v));
}

namespace detail {
inline void write_canonical_impl(std::uint64_t seq, std::string_view sym_field,
                                 const Event& e, std::string& out);
}

// Canonical JSON line (SCHEMA.md), no newline.
inline void write_canonical(std::uint64_t seq, const Event& e, std::string& out) {
    detail::write_canonical_impl(seq, "", e, out);
}

// Canonical line for `engine:true` vectors: `"symbol":N` after `ev`.
inline void write_canonical_sym(std::uint64_t seq, Symbol sym, const Event& e,
                                std::string& out) {
    std::string sf = ",\"symbol\":";
    append_u(sf, sym);
    detail::write_canonical_impl(seq, sf, e, out);
}

namespace detail {
inline void write_canonical_impl(std::uint64_t seq, std::string_view sym_field,
                                 const Event& e, std::string& out) {
    out += "{\"seq\":";
    append_u(out, seq);
    switch (e.kind) {
        case Event::Kind::Accepted:
            out += ",\"ev\":\"accepted\"";
            out += sym_field;
            out += ",\"order_id\":";
            append_u(out, e.order_id);
            out += ",\"leaves_qty\":";
            append_u(out, e.leaves_qty);
            break;
        case Event::Kind::Rejected:
            out += ",\"ev\":\"rejected\"";
            out += sym_field;
            out += ",\"order_id\":";
            append_u(out, e.order_id);
            out += ",\"reason\":\"";
            out += to_str(RejectReason(e.reason));
            out += '"';
            break;
        case Event::Kind::Trade:
            out += ",\"ev\":\"trade\"";
            out += sym_field;
            out += ",\"maker\":";
            append_u(out, e.maker);
            out += ",\"taker\":";
            append_u(out, e.taker);
            out += ",\"price\":";
            append_i(out, e.price);
            out += ",\"qty\":";
            append_u(out, e.qty);
            break;
        case Event::Kind::Closed:
            out += ",\"ev\":\"closed\"";
            out += sym_field;
            out += ",\"order_id\":";
            append_u(out, e.order_id);
            out += ",\"reason\":\"";
            out += to_str(CloseReason(e.reason));
            out += '"';
            break;
        case Event::Kind::Replaced:
            out += ",\"ev\":\"replaced\"";
            out += sym_field;
            out += ",\"order_id\":";
            append_u(out, e.order_id);
            out += ",\"price\":";
            append_i(out, e.price);
            out += ",\"qty\":";
            append_u(out, e.qty);
            break;
    }
    out += '}';
}
} // namespace detail

enum class IndexKind : std::uint8_t { Ladder, Tree };

struct BookConfig {
    Price price_min = 0;
    Price price_max = 1'000'000;
    std::size_t max_orders = 65'536;
    IndexKind index = IndexKind::Ladder;
};

} // namespace matcher
