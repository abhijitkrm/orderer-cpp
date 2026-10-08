// Minimal flat-JSON reader for vector/corpus lines — flat {"k":v,"k":"s"}
// objects only (no nesting/escapes needed for our grammar). Zero-dep.
#pragma once

#include <optional>
#include <string_view>

#include "types.hpp"

namespace matcher::jsonflat {

inline std::string_view get_str(std::string_view line, std::string_view key) {
    std::string pat = "\"" + std::string(key) + "\":";
    auto p = line.find(pat);
    if (p == std::string_view::npos) return {};
    auto rest = line.substr(p + pat.size());
    if (!rest.empty() && rest.front() == '"') {
        auto e = rest.find('"', 1);
        if (e == std::string_view::npos) return {};
        return rest.substr(1, e - 1);
    }
    auto e = rest.find_first_of(",}");
    return rest.substr(0, e);
}

inline std::optional<std::int64_t> get_i64(std::string_view line, std::string_view key) {
    auto s = get_str(line, key);
    if (s.empty()) return std::nullopt;
    std::int64_t v = 0, sign = 1;
    std::size_t i = 0;
    if (s[0] == '-') { sign = -1; i = 1; }
    for (; i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9') return std::nullopt;
        v = v * 10 + (s[i] - '0');
    }
    return v * sign;
}

inline std::optional<std::uint64_t> get_u64(std::string_view line, std::string_view key) {
    auto s = get_str(line, key);
    if (s.empty()) return std::nullopt;
    std::uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return std::nullopt;
        v = v * 10 + std::uint64_t(c - '0');
    }
    return v;
}

inline bool parse_command(std::string_view line, Command& out, Symbol* sym = nullptr) {
    if (sym) *sym = Symbol(get_u64(line, "symbol").value_or(0));
    auto cmd = get_str(line, "cmd");
    if (cmd == "new") {
        out = Command{};
        out.kind = Command::Kind::New;
        out.order_id = get_u64(line, "order_id").value_or(0);
        auto side = get_str(line, "side");
        out.side = side == "ask" ? Side::Ask : Side::Bid;
        out.otype = get_str(line, "otype") == "market" ? OType::Market : OType::Limit;
        auto t = get_str(line, "tif");
        out.tif = t == "ioc" ? Tif::Ioc : t == "fok" ? Tif::Fok
                  : t == "post_only" ? Tif::PostOnly : Tif::Gtc;
        out.price = get_i64(line, "price").value_or(0);
        out.qty = get_u64(line, "qty").value_or(0);
        return true;
    }
    if (cmd == "cancel") {
        out = Command::cancel(get_u64(line, "order_id").value_or(0));
        return true;
    }
    if (cmd == "replace") {
        out = Command::replace(get_u64(line, "order_id").value_or(0),
                               get_i64(line, "price").value_or(0),
                               get_u64(line, "qty").value_or(0));
        return true;
    }
    return false;
}

struct Header {
    Price pmin = 0, pmax = 1'000'000;
    std::size_t max_orders = 65'536;
    IndexKind index = IndexKind::Ladder;
    std::string index_raw = "ladder";
    bool engine = false;
};

inline Header parse_header(std::string_view line) {
    Header h;
    h.pmin = get_i64(line, "pmin").value_or(0);
    h.pmax = get_i64(line, "pmax").value_or(1'000'000);
    h.max_orders = std::size_t(get_u64(line, "max_orders").value_or(65'536));
    auto ir = get_str(line, "index");
    h.index_raw = ir.empty() ? "ladder" : std::string(ir);
    h.index = h.index_raw == "tree" ? IndexKind::Tree : IndexKind::Ladder;
    h.engine = get_str(line, "engine") == "true";
    return h;
}

} // namespace matcher::jsonflat
