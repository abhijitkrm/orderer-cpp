// Snapshot serialization per spec/JOURNAL.md — flat {"rec":...} lines:
//   {"format":"matcher-snap/1","pmin":P,"pmax":M,"max_orders":N,"index":"..."}
//   {"rec":"book","symbol":S,"seq":N}
//   {"rec":"order","order_id":I,"side":"...","otype":"limit","tif":"...",
//    "price":P,"qty":Q}
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "book.hpp"
#include "engine.hpp"
#include "jsonflat.hpp"

namespace matcher::snapshot {

inline void write_book(OrderBook& b, Symbol sym, std::string& out) {
    out += "{\"rec\":\"book\",\"symbol\":" + std::to_string(sym);
    out += ",\"seq\":" + std::to_string(b.seq()) + "}\n";
    for (const auto& o : b.resting_orders()) {
        out += "{\"rec\":\"order\",\"order_id\":" + std::to_string(o.order_id);
        out += ",\"side\":\"";
        out += o.side == Side::Ask ? "ask" : "bid";
        out += "\",\"otype\":\"limit\",\"tif\":\"";
        switch (o.tif) {
            case Tif::Ioc: out += "ioc"; break;
            case Tif::Fok: out += "fok"; break;
            case Tif::PostOnly: out += "post_only"; break;
            default: out += "gtc"; break;
        }
        out += "\",\"price\":" + std::to_string(o.price);
        out += ",\"qty\":" + std::to_string(o.qty) + "}\n";
    }
}

inline void write_engine(Engine& e, std::string& out) {
    const BookConfig& c = e.default_config();
    out += "{\"format\":\"matcher-snap/1\",\"pmin\":" + std::to_string(c.price_min);
    out += ",\"pmax\":" + std::to_string(c.price_max);
    out += ",\"max_orders\":" + std::to_string(c.max_orders);
    out += ",\"index\":\"";
    out += c.index == IndexKind::Tree ? "tree" : "ladder";
    out += "\"}\n";
    for (Symbol s : e.symbols()) write_book(*e.book(s), s, out);
}

struct SnapBook {
    Symbol symbol = 0;
    std::uint64_t seq = 0;
    std::vector<OrderBook::RestingOrder> orders;
};

struct Snap {
    BookConfig cfg{};
    std::vector<SnapBook> books;
};

/// Parse the flat record format written by write_engine.
inline Snap parse(std::string_view text) {
    Snap s;
    s.cfg = BookConfig{};
    SnapBook* cur = nullptr;
    std::size_t pos = 0;
    while (pos < text.size()) {
        auto eol = text.find('\n', pos);
        if (eol == std::string_view::npos) eol = text.size();
        auto line = text.substr(pos, eol - pos);
        pos = eol + 1;
        if (line.empty()) continue;
        using namespace jsonflat;
        if (line.find("\"rec\":\"order\"") != std::string_view::npos) {
            if (!cur) continue;
            OrderBook::RestingOrder o{};
            o.order_id = get_u64(line, "order_id").value_or(0);
            o.side = get_str(line, "side") == "ask" ? Side::Ask : Side::Bid;
            auto t = get_str(line, "tif");
            o.tif = t == "ioc" ? Tif::Ioc : t == "fok" ? Tif::Fok
                    : t == "post_only" ? Tif::PostOnly : Tif::Gtc;
            o.price = get_i64(line, "price").value_or(0);
            o.qty = get_u64(line, "qty").value_or(0);
            cur->orders.push_back(o);
        } else if (line.find("\"rec\":\"book\"") != std::string_view::npos) {
            s.books.push_back(SnapBook{});
            cur = &s.books.back();
            cur->symbol = Symbol(get_u64(line, "symbol").value_or(0));
            cur->seq = get_u64(line, "seq").value_or(0);
        } else {
            s.cfg.price_min = get_i64(line, "pmin").value_or(0);
            s.cfg.price_max = get_i64(line, "pmax").value_or(0);
            s.cfg.max_orders = std::size_t(get_u64(line, "max_orders").value_or(0));
            s.cfg.index = get_str(line, "index") == "tree" ? IndexKind::Tree
                                                          : IndexKind::Ladder;
        }
    }
    return s;
}

/// Rebuild an engine from a parsed snapshot.
inline Engine restore_engine(const Snap& s) {
    Engine e(s.cfg);
    for (const auto& b : s.books) {
        e.add_book(b.symbol, OrderBook::restore(s.cfg, b.seq, b.orders));
    }
    return e;
}

} // namespace matcher::snapshot
