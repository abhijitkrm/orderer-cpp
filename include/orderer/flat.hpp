// flat.hpp — strict flat-JSON field access and canonical command lines.
//
// matcher-cpp's jsonflat is lenient (bad fields become 0). orderer's
// harnesses must reject malformed input exactly as orderer-rust does
// (spec/HARNESS.md §5), so these mirror matcher-rust's jsonflat: a missing
// or unparsable field is an error.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <matcher/matcher.hpp>

namespace orderer::flat {

using namespace matcher;

inline std::string_view trim(std::string_view s) {
    auto ws = [](char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; };
    while (!s.empty() && ws(s.front())) s.remove_prefix(1);
    while (!s.empty() && ws(s.back())) s.remove_suffix(1);
    return s;
}

/// Value of `key` in a flat object line: the quoted string's contents, or
/// the trimmed unquoted token.
inline std::optional<std::string_view> get_str(std::string_view line, std::string_view key) {
    std::string pat;
    pat.reserve(key.size() + 3);
    pat += '"';
    pat += key;
    pat += "\":";
    auto p = line.find(pat);
    if (p == std::string_view::npos) return std::nullopt;
    auto rest = line.substr(p + pat.size());
    if (!rest.empty() && rest.front() == '"') {
        auto e = rest.find('"', 1);
        if (e == std::string_view::npos) return std::nullopt;
        return rest.substr(1, e - 1);
    }
    auto e = rest.find_first_of(",}");
    return trim(e == std::string_view::npos ? rest : rest.substr(0, e));
}

inline std::optional<std::uint64_t> parse_u64(std::string_view s) {
    if (!s.empty() && s.front() == '+') s.remove_prefix(1);
    if (s.empty()) return std::nullopt;
    std::uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return std::nullopt;
        std::uint64_t d = std::uint64_t(c - '0');
        if (v > (UINT64_MAX - d) / 10) return std::nullopt;
        v = v * 10 + d;
    }
    return v;
}

inline std::optional<std::int64_t> parse_i64(std::string_view s) {
    bool neg = false;
    if (!s.empty() && (s.front() == '-' || s.front() == '+')) {
        neg = s.front() == '-';
        s.remove_prefix(1);
    }
    auto u = parse_u64(s);
    if (!u || (!s.empty() && s.front() == '+')) return std::nullopt;
    if (neg) {
        if (*u > std::uint64_t(INT64_MAX) + 1) return std::nullopt;
        return std::int64_t(0 - *u);
    }
    if (*u > std::uint64_t(INT64_MAX)) return std::nullopt;
    return std::int64_t(*u);
}

inline std::optional<std::uint64_t> get_u64(std::string_view line, std::string_view key) {
    auto s = get_str(line, key);
    return s ? parse_u64(*s) : std::nullopt;
}

inline std::optional<std::int64_t> get_i64(std::string_view line, std::string_view key) {
    auto s = get_str(line, key);
    return s ? parse_i64(*s) : std::nullopt;
}

/// One canonical command line; nullopt if any field is missing or invalid.
inline std::optional<Command> parse_command(std::string_view line) {
    auto cmd = get_str(line, "cmd");
    if (!cmd) return std::nullopt;
    if (*cmd == "new") {
        auto side = get_str(line, "side");
        auto otype = get_str(line, "otype");
        auto tif = get_str(line, "tif");
        auto id = get_u64(line, "order_id");
        auto price = get_i64(line, "price");
        auto qty = get_u64(line, "qty");
        if (!side || !otype || !tif || !id || !price || !qty) return std::nullopt;
        Command c{};
        c.kind = Command::Kind::New;
        if (*side == "bid") c.side = Side::Bid;
        else if (*side == "ask") c.side = Side::Ask;
        else return std::nullopt;
        if (*otype == "limit") c.otype = OType::Limit;
        else if (*otype == "market") c.otype = OType::Market;
        else return std::nullopt;
        if (*tif == "gtc") c.tif = Tif::Gtc;
        else if (*tif == "ioc") c.tif = Tif::Ioc;
        else if (*tif == "fok") c.tif = Tif::Fok;
        else if (*tif == "post_only") c.tif = Tif::PostOnly;
        else return std::nullopt;
        c.order_id = *id;
        c.price = *price;
        c.qty = *qty;
        return c;
    }
    if (*cmd == "cancel") {
        auto id = get_u64(line, "order_id");
        if (!id) return std::nullopt;
        return Command::cancel(*id);
    }
    if (*cmd == "replace") {
        auto id = get_u64(line, "order_id");
        auto price = get_i64(line, "price");
        auto qty = get_u64(line, "qty");
        if (!id || !price || !qty) return std::nullopt;
        return Command::replace(*id, *price, *qty);
    }
    return std::nullopt;
}

/// Corpus/vector header → default book config (matcher defaults).
inline BookConfig parse_header(std::string_view line) {
    BookConfig c;
    c.price_min = get_i64(line, "pmin").value_or(0);
    c.price_max = get_i64(line, "pmax").value_or(1'000'000);
    c.max_orders = std::size_t(get_u64(line, "max_orders").value_or(65'536));
    auto ix = get_str(line, "index");
    c.index = (ix && *ix == "tree") ? IndexKind::Tree : IndexKind::Ladder;
    return c;
}

inline const char* index_name(IndexKind i) { return i == IndexKind::Tree ? "tree" : "ladder"; }

inline bool same_book(const BookConfig& a, const BookConfig& b) {
    return a.price_min == b.price_min && a.price_max == b.price_max &&
           a.max_orders == b.max_orders && a.index == b.index;
}

/// matcher's canonical command line (SCHEMA.md), engine form when `sym`.
inline void write_command(const Command& c, const Symbol* sym, std::string& out) {
    auto symf = [&] {
        if (sym) {
            out += ",\"symbol\":";
            append_u(out, *sym);
        }
    };
    switch (c.kind) {
        case Command::Kind::New:
            out += "{\"cmd\":\"new\"";
            symf();
            out += ",\"order_id\":";
            append_u(out, c.order_id);
            out += ",\"side\":\"";
            out += to_str(c.side);
            out += "\",\"otype\":\"";
            out += to_str(c.otype);
            out += "\",\"price\":";
            append_i(out, c.price);
            out += ",\"qty\":";
            append_u(out, c.qty);
            out += ",\"tif\":\"";
            out += to_str(c.tif);
            out += "\"}";
            break;
        case Command::Kind::Cancel:
            out += "{\"cmd\":\"cancel\"";
            symf();
            out += ",\"order_id\":";
            append_u(out, c.order_id);
            out += '}';
            break;
        case Command::Kind::Replace:
            out += "{\"cmd\":\"replace\"";
            symf();
            out += ",\"order_id\":";
            append_u(out, c.order_id);
            out += ",\"price\":";
            append_i(out, c.price);
            out += ",\"qty\":";
            append_u(out, c.qty);
            out += '}';
            break;
    }
}

}  // namespace orderer::flat
