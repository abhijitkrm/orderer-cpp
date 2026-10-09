// journal.hpp — per-partition journals (spec/JOURNAL.md): naming, JSONL and
// binary encodings, strict readers, and the asynchronous chunk writer.
#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "core.hpp"
#include "flat.hpp"

namespace orderer {

namespace fs = std::filesystem;

enum class JournalFormat : std::uint8_t { Jsonl, Binary };
enum class Kind : std::uint8_t { Cmd = 1, Evt = 2 };

/// When the I/O thread fsyncs. Never observable (spec/PIPELINE.md §8).
struct FsyncPolicy {
    enum class Mode : std::uint8_t { Never, EveryN, Every } mode = Mode::EveryN;
    std::uint64_t n = 1024;
    std::chrono::nanoseconds idle{std::chrono::microseconds(200)};
    std::chrono::nanoseconds interval{0};

    static FsyncPolicy never() { return {Mode::Never}; }
    /// Group commit once n records are pending, or after `idle` with no more.
    static FsyncPolicy every_n(std::uint64_t n) { return {Mode::EveryN, n}; }
    static FsyncPolicy every(std::chrono::nanoseconds iv) {
        FsyncPolicy p{Mode::Every};
        p.interval = iv;
        p.idle = iv;
        return p;
    }
};

struct JournalConfig {
    fs::path dir;
    JournalFormat format = JournalFormat::Binary;
    FsyncPolicy fsync = FsyncPolicy::every_n(1024);
    bool events = true;   // also write evt-{p}
    bool append = false;  // append (after recovery, same P) instead of truncate
};

struct CorruptJournal : std::runtime_error {
    using std::runtime_error::runtime_error;
};

inline const char* kind_name(Kind k) { return k == Kind::Cmd ? "cmd" : "evt"; }
/// Version-2 record sizes (1.2): the version-1 record + CRC-32C + 4 reserved bytes.
inline constexpr std::size_t HEADER = 64, CMD_RECORD = 48, EVT_RECORD = 56;
inline constexpr std::size_t CMD_RECORD_V1 = 40, EVT_RECORD_V1 = 48;
inline constexpr std::uint16_t VERSION = 2;
inline std::size_t record_size(Kind k, std::uint16_t version = VERSION) {
    if (version == 1) return k == Kind::Cmd ? CMD_RECORD_V1 : EVT_RECORD_V1;
    return k == Kind::Cmd ? CMD_RECORD : EVT_RECORD;
}
/// Bytes the checksum covers (the version-1 record).
inline std::size_t payload_size(Kind k) { return k == Kind::Cmd ? CMD_RECORD_V1 : EVT_RECORD_V1; }

/// CRC-32C (Castagnoli, reflected 0x82F63B78), spec/JOURNAL.md §2.2.
inline std::uint32_t crc32c(const std::uint8_t* d, std::size_t n) {
    static const std::array<std::uint32_t, 256> table = [] {
        std::array<std::uint32_t, 256> t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
            t[i] = c;
        }
        return t;
    }();
    std::uint32_t c = ~0u;
    for (std::size_t i = 0; i < n; ++i) c = table[(c ^ d[i]) & 0xFF] ^ (c >> 8);
    return ~c;
}

inline const char* ext_of(JournalFormat f) { return f == JournalFormat::Jsonl ? "journal" : "bin"; }

/// spec/JOURNAL.md §1: the segment starting after cut `start` (0 = the base file).
inline fs::path segment_path(const fs::path& dir, Kind k, std::uint32_t p, std::uint64_t start, JournalFormat f) {
    std::string n = std::string(kind_name(k)) + "-" + std::to_string(p);
    if (start) n += "." + std::to_string(start);
    return dir / (n + "." + ext_of(f));
}

inline fs::path journal_path(const fs::path& dir, Kind k, std::uint32_t p, JournalFormat f) {
    return segment_path(dir, k, p, 0, f);
}

struct Segment {
    std::uint32_t partition;
    std::uint64_t start;
    fs::path path;
    bool operator<(const Segment& o) const {
        return partition != o.partition ? partition < o.partition : start < o.start;
    }
};

inline bool parse_uint(std::string_view s, std::uint64_t& out) {
    if (s.empty() || s.size() > 20) return false;
    std::uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        std::uint64_t nv = v * 10 + std::uint64_t(c - '0');
        if (nv / 10 != v) return false;
        v = nv;
    }
    out = v;
    return true;
}

/// Every `k` segment in `dir`, sorted by (partition, start).
inline std::vector<Segment> list_segments(const fs::path& dir, Kind k, JournalFormat f) {
    std::vector<Segment> out;
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) return out;
    std::string prefix = std::string(kind_name(k)) + "-", suffix = std::string(".") + ext_of(f);
    for (auto& e : fs::directory_iterator(dir, ec)) {
        std::string name = e.path().filename().string();
        if (name.size() <= prefix.size() + suffix.size() || name.compare(0, prefix.size(), prefix) != 0 ||
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0)
            continue;
        std::string_view mid(name);
        mid = mid.substr(prefix.size(), mid.size() - prefix.size() - suffix.size());
        auto dot = mid.find('.');
        std::uint64_t p = 0, start = 0;
        if (!parse_uint(mid.substr(0, dot), p) || p > UINT32_MAX) continue;
        if (dot != std::string_view::npos && (!parse_uint(mid.substr(dot + 1), start) || start == 0)) continue;
        out.push_back({std::uint32_t(p), start, e.path()});
    }
    std::sort(out.begin(), out.end());
    return out;
}

/// spec/JOURNAL.md §6: checkpoint snapshot path for cut `n`.
inline fs::path checkpoint_path(const fs::path& dir, std::uint64_t n) {
    return dir / ("checkpoint-" + std::to_string(n) + ".snap");
}

inline fs::path meta_of(const fs::path& p) { return fs::path(p.string() + ".meta"); }

/// Checkpoints in `dir` with cut below `below`, ascending; `complete` = with sidecar.
inline std::vector<std::pair<std::uint64_t, fs::path>> list_checkpoints(const fs::path& dir, bool complete = true,
                                                                        std::uint64_t below = UINT64_MAX) {
    std::vector<std::pair<std::uint64_t, fs::path>> out;
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) return out;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        std::string name = e.path().filename().string();
        if (name.size() < 17 || name.compare(0, 11, "checkpoint-") != 0 || name.compare(name.size() - 5, 5, ".snap") != 0)
            continue;
        std::uint64_t n = 0;
        if (!parse_uint(std::string_view(name).substr(11, name.size() - 16), n) || n >= below) continue;
        if (complete && !fs::exists(meta_of(e.path()))) continue;
        out.push_back({n, e.path()});
    }
    std::sort(out.begin(), out.end());
    return out;
}

// ---- encodings ----------------------------------------------------------------------

inline std::string jsonl_header(Kind k, std::uint32_t p, std::uint32_t P, const BookConfig& b) {
    std::string s = "{\"format\":\"orderer-journal/1\",\"kind\":\"";
    s += kind_name(k);
    s += "\",\"partition\":";
    append_u(s, p);
    s += ",\"partitions\":";
    append_u(s, P);
    s += ",\"pmin\":";
    append_i(s, b.price_min);
    s += ",\"pmax\":";
    append_i(s, b.price_max);
    s += ",\"max_orders\":";
    append_u(s, b.max_orders);
    s += ",\"index\":\"";
    s += flat::index_name(b.index);
    s += "\"}\n";
    return s;
}

inline void put_u16(std::uint8_t* d, std::uint16_t v) { for (int i = 0; i < 2; ++i) d[i] = std::uint8_t(v >> (8 * i)); }
inline void put_u32(std::uint8_t* d, std::uint32_t v) { for (int i = 0; i < 4; ++i) d[i] = std::uint8_t(v >> (8 * i)); }
inline void put_u64(std::uint8_t* d, std::uint64_t v) { for (int i = 0; i < 8; ++i) d[i] = std::uint8_t(v >> (8 * i)); }
inline std::uint32_t get_u32le(const std::uint8_t* d) { std::uint32_t v = 0; for (int i = 3; i >= 0; --i) v = (v << 8) | d[i]; return v; }
inline std::uint64_t get_u64le(const std::uint8_t* d) { std::uint64_t v = 0; for (int i = 7; i >= 0; --i) v = (v << 8) | d[i]; return v; }

inline std::array<std::uint8_t, HEADER> binary_header(Kind k, std::uint32_t p, std::uint32_t P, const BookConfig& b) {
    std::array<std::uint8_t, HEADER> h{};
    std::memcpy(h.data(), "ORDJ", 4);
    put_u16(h.data() + 4, VERSION);
    h[6] = std::uint8_t(k);
    h[7] = b.index == IndexKind::Tree ? 1 : 0;
    put_u32(h.data() + 8, p);
    put_u32(h.data() + 12, P);
    put_u32(h.data() + 16, std::uint32_t(record_size(k)));
    put_u64(h.data() + 24, std::uint64_t(b.price_min));
    put_u64(h.data() + 32, std::uint64_t(b.price_max));
    put_u64(h.data() + 40, std::uint64_t(b.max_orders));
    return h;
}

/// JSONL command record line (no newline): canonical engine line + "iseq".
inline void write_cmd_line(std::uint64_t iseq, Symbol sym, const Command& c, std::string& out) {
    flat::write_command(c, &sym, out);
    out.pop_back();
    out += ",\"iseq\":";
    append_u(out, iseq);
    out += '}';
}

inline std::uint8_t tif_code(Tif t) { return std::uint8_t(t); }  // gtc 0, ioc 1, fok 2, post_only 3

/// Seal a version-2 record: CRC-32C of the payload, then zeros.
inline void seal(std::uint8_t* r, std::size_t payload) {
    put_u32(r + payload, crc32c(r, payload));
    put_u32(r + payload + 4, 0);
}
inline bool sealed(const std::uint8_t* r, std::size_t payload) { return get_u32le(r + payload) == crc32c(r, payload); }

/// spec/JOURNAL.md §2.2 command record (version 2, sealed; CMD_RECORD bytes).
inline void encode_cmd(std::uint64_t iseq, Symbol sym, const Command& c, std::uint8_t* r) {
    std::memset(r, 0, CMD_RECORD);
    put_u64(r, iseq);
    put_u32(r + 8, sym);
    put_u64(r + 16, c.order_id);
    switch (c.kind) {
        case Command::Kind::New:
            r[12] = 1;
            r[13] = c.side == Side::Ask ? 1 : 0;
            r[14] = c.otype == OType::Market ? 1 : 0;
            r[15] = tif_code(c.tif);
            put_u64(r + 24, std::uint64_t(c.price));
            put_u64(r + 32, c.qty);
            break;
        case Command::Kind::Cancel: r[12] = 2; break;
        case Command::Kind::Replace:
            r[12] = 3;
            put_u64(r + 24, std::uint64_t(c.price));
            put_u64(r + 32, c.qty);
            break;
    }
    seal(r, CMD_RECORD_V1);
}

struct CmdRecord {
    std::uint64_t iseq;
    Symbol sym;
    Command cmd;
};

inline std::optional<CmdRecord> decode_cmd(const std::uint8_t* r) {
    CmdRecord rec{get_u64le(r), get_u32le(r + 8), Command{}};
    Command& c = rec.cmd;
    c.order_id = get_u64le(r + 16);
    switch (r[12]) {
        case 1:
            c.kind = Command::Kind::New;
            if (r[13] > 1 || r[14] > 1 || r[15] > 3) return std::nullopt;
            c.side = r[13] ? Side::Ask : Side::Bid;
            c.otype = r[14] ? OType::Market : OType::Limit;
            c.tif = Tif(r[15]);
            c.price = std::int64_t(get_u64le(r + 24));
            c.qty = get_u64le(r + 32);
            break;
        case 2: c = Command::cancel(c.order_id); break;
        case 3: c = Command::replace(c.order_id, std::int64_t(get_u64le(r + 24)), get_u64le(r + 32)); break;
        default: return std::nullopt;
    }
    return rec;
}

/// spec/JOURNAL.md §3.2: reject 1..7 in SPEC order, close 1..3.
inline void encode_evt(std::uint64_t seq, Symbol sym, const Event& e, std::uint8_t* r) {
    std::memset(r, 0, EVT_RECORD);
    put_u64(r, seq);
    put_u32(r + 8, sym);
    std::uint64_t a = 0, b = 0, d = 0;
    std::int64_t c = 0;
    switch (e.kind) {
        case Event::Kind::Accepted: r[12] = 1; a = e.order_id; d = e.leaves_qty; break;
        case Event::Kind::Rejected: r[12] = 2; r[13] = std::uint8_t(e.reason + 1); a = e.order_id; break;
        case Event::Kind::Trade: r[12] = 3; a = e.maker; b = e.taker; c = e.price; d = e.qty; break;
        case Event::Kind::Closed: r[12] = 4; r[13] = std::uint8_t(e.reason + 1); a = e.order_id; break;
        case Event::Kind::Replaced: r[12] = 5; a = e.order_id; c = e.price; d = e.qty; break;
    }
    put_u64(r + 16, a);
    put_u64(r + 24, b);
    put_u64(r + 32, std::uint64_t(c));
    put_u64(r + 40, d);
    seal(r, EVT_RECORD_V1);
}

inline std::optional<std::pair<std::uint64_t, std::pair<Symbol, Event>>> decode_evt(const std::uint8_t* r) {
    Event e{};
    std::uint64_t a = get_u64le(r + 16), b = get_u64le(r + 24), d = get_u64le(r + 40);
    std::int64_t c = std::int64_t(get_u64le(r + 32));
    std::uint8_t reason = r[13];
    switch (r[12]) {
        case 1: if (reason) return std::nullopt; e.kind = Event::Kind::Accepted; e.order_id = a; e.leaves_qty = d; break;
        case 2: if (reason < 1 || reason > 7) return std::nullopt; e.kind = Event::Kind::Rejected; e.order_id = a; e.reason = std::uint8_t(reason - 1); break;
        case 3: if (reason) return std::nullopt; e.kind = Event::Kind::Trade; e.maker = a; e.taker = b; e.price = c; e.qty = d; break;
        case 4: if (reason < 1 || reason > 3) return std::nullopt; e.kind = Event::Kind::Closed; e.order_id = a; e.reason = std::uint8_t(reason - 1); break;
        case 5: if (reason) return std::nullopt; e.kind = Event::Kind::Replaced; e.order_id = a; e.price = c; e.qty = d; break;
        default: return std::nullopt;
    }
    return std::make_pair(get_u64le(r), std::make_pair(Symbol(get_u32le(r + 8)), e));
}

inline constexpr std::size_t MAX_RECORD = 256;

inline void push_cmd(std::string& out, JournalFormat f, std::uint64_t iseq, Symbol sym, const Command& c) {
    if (f == JournalFormat::Binary) {
        std::uint8_t r[CMD_RECORD];
        encode_cmd(iseq, sym, c, r);
        out.append(reinterpret_cast<const char*>(r), CMD_RECORD);
    } else {
        write_cmd_line(iseq, sym, c, out);
        out += '\n';
    }
}

inline void push_evt(std::string& out, JournalFormat f, std::uint64_t seq, Symbol sym, const Event& e) {
    if (f == JournalFormat::Binary) {
        std::uint8_t r[EVT_RECORD];
        encode_evt(seq, sym, e, r);
        out.append(reinterpret_cast<const char*>(r), EVT_RECORD);
    } else {
        write_canonical_sym(seq, sym, e, out);
        out += '\n';
    }
}

// ---- reading (recovery) ------------------------------------------------------------------

struct JournalHeader {
    Kind kind;
    std::uint32_t partition, partitions;
    BookConfig book;
    std::uint16_t version = VERSION;  // binary journal version; JSONL reports 2
    bool operator==(const JournalHeader& o) const {
        return kind == o.kind && partition == o.partition && partitions == o.partitions &&
               flat::same_book(book, o.book);
    }
};

[[noreturn]] inline void corrupt(const fs::path& p, const std::string& d) {
    throw CorruptJournal(p.string() + ": " + d);
}

inline std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) corrupt(p, std::strerror(errno));
    return std::string(std::istreambuf_iterator<char>(in), {});
}

inline JournalHeader parse_jsonl_header(const fs::path& p, std::string_view line) {
    using namespace flat;
    if (get_str(line, "format") != std::optional<std::string_view>("orderer-journal/1"))
        corrupt(p, "not an orderer-journal/1 header");
    JournalHeader h{};
    auto k = get_str(line, "kind");
    if (k && *k == "cmd") h.kind = Kind::Cmd;
    else if (k && *k == "evt") h.kind = Kind::Evt;
    else corrupt(p, "bad kind");
    auto num = [&](const char* key) {
        auto v = get_u64(line, key);
        if (!v || *v > UINT32_MAX) corrupt(p, std::string("bad ") + key);
        return std::uint32_t(*v);
    };
    h.partition = num("partition");
    h.partitions = num("partitions");
    auto pmin = get_i64(line, "pmin"), pmax = get_i64(line, "pmax");
    auto mo = get_u64(line, "max_orders");
    auto ix = get_str(line, "index");
    if (!pmin) corrupt(p, "bad pmin");
    if (!pmax) corrupt(p, "bad pmax");
    if (!mo) corrupt(p, "bad max_orders");
    if (!ix || (*ix != "ladder" && *ix != "tree")) corrupt(p, "bad index");
    h.book = BookConfig{*pmin, *pmax, std::size_t(*mo), *ix == "tree" ? IndexKind::Tree : IndexKind::Ladder};
    h.version = VERSION;
    return h;
}

inline JournalHeader parse_binary_header(const fs::path& p, const std::string& bytes) {
    if (bytes.size() < HEADER || bytes.compare(0, 4, "ORDJ") != 0) corrupt(p, "bad magic");
    auto d = reinterpret_cast<const std::uint8_t*>(bytes.data());
    std::uint16_t version = std::uint16_t(d[4] | (d[5] << 8));
    if (version != 1 && version != 2) corrupt(p, "unsupported version");
    JournalHeader h{};
    h.version = version;
    if (d[6] == 1) h.kind = Kind::Cmd;
    else if (d[6] == 2) h.kind = Kind::Evt;
    else corrupt(p, "bad kind");
    if (get_u32le(d + 16) != record_size(h.kind, version)) corrupt(p, "bad record_size");
    if (d[7] > 1) corrupt(p, "bad index");
    h.partition = get_u32le(d + 8);
    h.partitions = get_u32le(d + 12);
    h.book = BookConfig{std::int64_t(get_u64le(d + 24)), std::int64_t(get_u64le(d + 32)),
                        std::size_t(get_u64le(d + 40)), d[7] ? IndexKind::Tree : IndexKind::Ladder};
    return h;
}

inline JournalHeader read_header(const fs::path& p, JournalFormat f) {
    std::string bytes = read_file(p);
    if (f == JournalFormat::Binary) return parse_binary_header(p, bytes);
    return parse_jsonl_header(p, std::string_view(bytes).substr(0, bytes.find('\n')));
}

/// Strict (default) or repair reading (spec/JOURNAL.md §5, §5.1).
enum class ReadMode : std::uint8_t { Strict, Repair };

/// One file's records as byte ranges (binary records already checksum-checked).
struct Body {
    JournalHeader header;
    std::vector<std::pair<std::size_t, std::size_t>> records;  // offset, length
    std::size_t valid_len = 0;                                 // bytes a repair keeps
};

inline Body split_body(const fs::path& p, const std::string& bytes, JournalFormat f, ReadMode mode) {
    Body b;
    if (f == JournalFormat::Binary) {
        b.header = parse_binary_header(p, bytes);
        std::size_t size = record_size(b.header.kind, b.header.version), body = bytes.size() - HEADER;
        std::size_t n = body / size;
        if (body % size && mode == ReadMode::Strict) corrupt(p, "torn tail (partial record)");
        auto d = reinterpret_cast<const std::uint8_t*>(bytes.data());
        if (b.header.version >= 2) {
            for (std::size_t i = 0; i < n; ++i) {
                if (!sealed(d + HEADER + i * size, payload_size(b.header.kind))) {
                    if (mode == ReadMode::Repair && i + 1 == n) { --n; break; }  // a torn final record (§5.1)
                    corrupt(p, "record " + std::to_string(i) + ": checksum mismatch");
                }
            }
        }
        for (std::size_t i = 0; i < n; ++i) b.records.push_back({HEADER + i * size, size});
        b.valid_len = HEADER + n * size;
        return b;
    }
    std::size_t end = bytes.size();
    if (end && bytes[end - 1] != '\n') {
        if (mode == ReadMode::Strict) corrupt(p, "torn tail (final line has no newline)");
        auto nl = bytes.rfind('\n');
        end = nl == std::string::npos ? 0 : nl + 1;
    }
    std::size_t first = std::min(bytes.find('\n'), end);
    b.header = parse_jsonl_header(p, std::string_view(bytes).substr(0, first));
    for (std::size_t pos = first + 1; pos < end;) {
        auto e = bytes.find('\n', pos);
        b.records.push_back({pos, e - pos});
        pos = e + 1;
    }
    b.valid_len = end;
    return b;
}

inline std::vector<CmdRecord> decode_cmds(const fs::path& p, const std::string& bytes, JournalFormat f, const Body& b) {
    std::vector<CmdRecord> recs;
    recs.reserve(b.records.size());
    for (std::size_t i = 0; i < b.records.size(); ++i) {
        auto [off, len] = b.records[i];
        if (f == JournalFormat::Binary) {
            auto r = decode_cmd(reinterpret_cast<const std::uint8_t*>(bytes.data()) + off);
            if (!r) corrupt(p, "record " + std::to_string(i) + ": bad codes");
            recs.push_back(*r);
        } else {
            std::string_view l(bytes.data() + off, len);
            auto iseq = flat::get_u64(l, "iseq");
            auto sym = flat::get_u64(l, "symbol");
            auto cmd = flat::parse_command(l);
            if (!iseq || !sym || *sym > UINT32_MAX || !cmd)
                corrupt(p, "line " + std::to_string(i + 2) + ": malformed record: " + std::string(l));
            recs.push_back({*iseq, Symbol(*sym), *cmd});
        }
    }
    return recs;
}

inline void check_increasing(const fs::path& p, const std::vector<CmdRecord>& recs, std::optional<std::uint64_t> after) {
    for (auto& r : recs) {
        if (after && r.iseq <= *after)
            corrupt(p, "iseq not increasing (" + std::to_string(*after) + " then " + std::to_string(r.iseq) + ")");
        after = r.iseq;
    }
}

/// One command journal file, strictly (torn tails, bad records and
/// checksums, non-increasing iseq are corruption).
inline std::pair<JournalHeader, std::vector<CmdRecord>> read_cmd_journal(const fs::path& p, JournalFormat f) {
    std::string bytes = read_file(p);
    Body b = split_body(p, bytes, f, ReadMode::Strict);
    if (b.header.kind != Kind::Cmd) corrupt(p, "not a command journal");
    auto recs = decode_cmds(p, bytes, f, b);
    check_increasing(p, recs, std::nullopt);
    return {b.header, std::move(recs)};
}

/// Every partition's command journal in `dir`, all segments in order
/// (spec/JOURNAL.md §1, §5 step 3).
inline std::pair<JournalHeader, std::vector<std::vector<CmdRecord>>> read_cmd_dir(const fs::path& dir, JournalFormat f) {
    auto segs = list_segments(dir, Kind::Cmd, f);
    if (segs.empty()) corrupt(journal_path(dir, Kind::Cmd, 0, f), "no command journal");
    JournalHeader h0 = read_header(segs[0].path, f);
    std::vector<std::vector<CmdRecord>> all(h0.partitions);
    std::vector<bool> seen(h0.partitions, false);
    for (auto& s : segs) {
        auto [h, recs] = read_cmd_journal(s.path, f);
        if (h.partition != s.partition || s.partition >= h0.partitions || h.partitions != h0.partitions ||
            !flat::same_book(h.book, h0.book))
            corrupt(s.path, "header does not match its file name, partition count or book config");
        auto& part = all[s.partition];
        check_increasing(s.path, recs,
                         part.empty() ? std::nullopt : std::optional<std::uint64_t>(part.back().iseq));
        part.insert(part.end(), recs.begin(), recs.end());
        seen[s.partition] = true;
    }
    for (std::uint32_t p = 0; p < h0.partitions; ++p)
        if (!seen[p]) corrupt(journal_path(dir, Kind::Cmd, p, f), "partition has no journal");
    return {h0, std::move(all)};
}

/// An event journal file as canonical symbol-tagged lines.
inline std::vector<std::string> read_evt_journal(const fs::path& p, JournalFormat f) {
    std::string bytes = read_file(p);
    Body b = split_body(p, bytes, f, ReadMode::Strict);
    std::vector<std::string> out;
    for (std::size_t i = 0; i < b.records.size(); ++i) {
        auto [off, len] = b.records[i];
        if (f == JournalFormat::Binary) {
            auto r = decode_evt(reinterpret_cast<const std::uint8_t*>(bytes.data()) + off);
            if (!r) corrupt(p, "record " + std::to_string(i) + ": bad codes");
            std::string l;
            write_canonical_sym(r->first, r->second.first, r->second.second, l);
            out.push_back(std::move(l));
        } else {
            out.emplace_back(bytes.data() + off, len);
        }
    }
    return out;
}

/// A partition's whole event journal (all segments, in order).
inline std::vector<std::string> read_evt_partition(const fs::path& dir, JournalFormat f, std::uint32_t p) {
    std::vector<std::string> out;
    for (auto& s : list_segments(dir, Kind::Evt, f))
        if (s.partition == p) {
            auto l = read_evt_journal(s.path, f);
            out.insert(out.end(), l.begin(), l.end());
        }
    return out;
}

/// spec/JOURNAL.md §5.1: truncate a torn tail off each journal family's last
/// segment, in place. Returns (file, bytes removed) per truncation.
inline std::vector<std::pair<fs::path, std::uint64_t>> repair_dir(const fs::path& dir, JournalFormat f) {
    std::vector<std::pair<fs::path, std::uint64_t>> out;
    for (Kind k : {Kind::Cmd, Kind::Evt}) {
        std::map<std::uint32_t, fs::path> last;
        for (auto& s : list_segments(dir, k, f)) last[s.partition] = s.path;
        for (auto& [p, path] : last) {
            std::string bytes = read_file(path);
            Body b = split_body(path, bytes, f, ReadMode::Repair);
            if (b.valid_len < bytes.size()) {
                std::error_code ec;
                fs::resize_file(path, b.valid_len, ec);
                if (ec) corrupt(path, ec.message());
                int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
                if (fd >= 0) { ::fsync(fd); ::close(fd); }
                out.push_back({path, bytes.size() - b.valid_len});
            }
        }
    }
    return out;
}

// ---- writing -------------------------------------------------------------------------------

inline int open_fd(const fs::path& path, int flags) {
    int fd = ::open(path.c_str(), flags | O_CLOEXEC, 0644);
    if (fd < 0) throw std::runtime_error(path.string() + ": " + std::strerror(errno));
    return fd;
}

/// Create segment `start` (truncating any old file) with its header; fd at end.
inline int open_segment(const fs::path& dir, JournalFormat f, Kind k, std::uint32_t p, std::uint32_t P,
                        const BookConfig& book, std::uint64_t start) {
    auto path = segment_path(dir, k, p, start, f);
    int fd = open_fd(path, O_WRONLY | O_CREAT | O_TRUNC);
    std::string h;
    if (f == JournalFormat::Jsonl) h = jsonl_header(k, p, P, book);
    else { auto b = binary_header(k, p, P, book); h.assign(reinterpret_cast<const char*>(b.data()), b.size()); }
    if (::write(fd, h.data(), h.size()) != ssize_t(h.size()))
        throw std::runtime_error(path.string() + ": header write failed");
    return fd;
}

/// Append mode: the partition's last segment (header checked); otherwise a
/// fresh segment 0 (callers clear the directory once first).
inline int open_journal(const JournalConfig& cfg, Kind k, std::uint32_t p, std::uint32_t P, const BookConfig& book) {
    if (cfg.append) {
        std::optional<fs::path> last;
        for (auto& s : list_segments(cfg.dir, k, cfg.format))
            if (s.partition == p) last = s.path;
        if (last) {
            JournalHeader want{k, p, P, book};
            JournalHeader h = read_header(*last, cfg.format);
            if (!(h == want)) corrupt(*last, "header does not match the pipeline");
            if (cfg.format == JournalFormat::Binary && h.version != VERSION)
                corrupt(*last, "cannot append to a version-1 journal");
            return open_fd(*last, O_WRONLY | O_APPEND);
        }
    }
    return open_segment(cfg.dir, cfg.format, k, p, P, book, 0);
}

/// Remove checkpoints with cut below `n` (body first, so a half-removed pair
/// is never a complete checkpoint).
inline void remove_checkpoints_below(const fs::path& dir, std::uint64_t n) {
    for (auto& [cut, path] : list_checkpoints(dir, false, n)) {
        fs::remove(path);
        fs::remove(meta_of(path));
    }
}

/// spec/JOURNAL.md §6 step 4: remove segments that start below `n`.
inline void remove_segments_below(const fs::path& dir, JournalFormat f, std::uint64_t n) {
    for (Kind k : {Kind::Cmd, Kind::Evt})
        for (auto& s : list_segments(dir, k, f))
            if (s.start < n) fs::remove(s.path);
}

/// A fresh (non-append) pipeline owns its directory's journals.
inline void clear_journal_dir(const fs::path& dir, JournalFormat f) {
    remove_segments_below(dir, f, UINT64_MAX);
    remove_checkpoints_below(dir, UINT64_MAX);
}

/// Write `contents` durably: temporary name, sync, rename, sync the directory.
inline void write_durably(const fs::path& path, const std::string& contents) {
    fs::path tmp(path.string() + ".tmp");
    int fd = open_fd(tmp, O_WRONLY | O_CREAT | O_TRUNC);
    std::size_t off = 0;
    while (off < contents.size()) {
        ssize_t n = ::write(fd, contents.data() + off, contents.size() - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            throw std::runtime_error(tmp.string() + ": " + std::strerror(errno));
        }
        off += std::size_t(n);
    }
    if (::fsync(fd) != 0) { ::close(fd); throw std::runtime_error(tmp.string() + ": fsync failed"); }
    ::close(fd);
    fs::rename(tmp, path);
    int dfd = ::open(path.parent_path().empty() ? "." : path.parent_path().c_str(), O_RDONLY | O_CLOEXEC);
    if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
}

/// The platform's real durability primitive.
inline int durable_sync(int fd) {
#if defined(__APPLE__)
    if (::fcntl(fd, F_FULLFSYNC) == 0) return 0;
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

/// Watermarks an I/O thread advances.
struct Marks {
    std::shared_ptr<std::atomic<std::uint64_t>> flushed, durable;
};

/// Asynchronous journal writer: the owning thread encodes into a chunk (a
/// memcpy); full or idle chunks go to a dedicated I/O thread that writes and
/// group-commits fsyncs. Chunks and queues are preallocated — no allocation
/// in steady state, and no write/fsync stall reaches the owning thread.
class ChunkWriter {
  public:
    static constexpr std::size_t CHUNK = 1 << 18, CHUNKS = 64;

    ChunkWriter(int fd, std::optional<FsyncPolicy> fsync, Marks marks)
        : fd_(fd), fsync_(fsync), marks_(std::move(marks)) {
        chunks_.resize(CHUNKS);
        for (auto& c : chunks_) c.reserve(CHUNK);
        for (std::size_t i = 1; i < CHUNKS; ++i) free_.push(i);
        cur_ = 0;
        io_ = std::thread([this] { io_loop(); });
    }
    ChunkWriter(const ChunkWriter&) = delete;
    ~ChunkWriter() { finish(); }

    void reserve(std::size_t len) {
        if (chunks_[cur_].size() + len > CHUNK) hand_off();
    }
    std::string& buf() { return chunks_[cur_]; }
    void record(std::uint64_t id) { last_ = id; ++records_; }
    std::size_t pending() const { return chunks_[cur_].size(); }

    /// Hand the current chunk to the I/O thread (blocks only when every
    /// chunk is in flight — backpressure).
    void hand_off() {
        if (chunks_[cur_].empty()) return;
        to_io_.push(Msg{cur_, last_, records_, false, -1});
        records_ = 0;
        cur_ = free_.pop();
    }

    /// Continue in `next_fd` (a new segment, header written): everything so
    /// far goes to the current file, which the I/O thread syncs (per policy)
    /// and closes (spec/JOURNAL.md §6 step 2).
    void rotate(int next_fd) {
        hand_off();
        to_io_.push(Msg{0, 0, 0, false, next_fd});
    }

    /// Write and (per policy) sync everything; returns the first I/O error.
    std::optional<std::string> finish() {
        if (io_.joinable()) {
            hand_off();
            to_io_.push(Msg{0, 0, 0, true, -1});
            io_.join();
            ::close(fd_);
        }
        return error_;
    }

  private:
    struct Msg {
        std::size_t chunk;
        std::uint64_t last, records;
        bool stop;
        int rotate_fd;  // >= 0: switch to this file
    };
    /// Fixed-capacity blocking queue (no allocation after construction).
    template <class T>
    struct Queue {
        std::array<T, CHUNKS + 2> items{};
        std::size_t head = 0, len = 0;
        std::mutex m;
        std::condition_variable cv;
        void push(T v) {
            std::unique_lock<std::mutex> g(m);
            cv.wait(g, [&] { return len < items.size(); });
            items[(head + len) % items.size()] = v;
            ++len;
            cv.notify_all();
        }
        T pop() {
            std::unique_lock<std::mutex> g(m);
            cv.wait(g, [&] { return len > 0; });
            return take();
        }
        bool pop_for(T& out, std::chrono::nanoseconds d) {
            std::unique_lock<std::mutex> g(m);
            if (!cv.wait_for(g, d, [&] { return len > 0; })) return false;
            out = take();
            return true;
        }
        bool try_pop(T& out) {
            std::lock_guard<std::mutex> g(m);
            if (!len) return false;
            out = take();
            return true;
        }
        T take() {
            T v = items[head];
            head = (head + 1) % items.size();
            --len;
            cv.notify_all();
            return v;
        }
    };

    void write_all(const std::string& b) {
        std::size_t off = 0;
        while (off < b.size() && !error_) {
            ssize_t n = ::write(fd_, b.data() + off, b.size() - off);
            if (n < 0) {
                if (errno == EINTR) continue;
                error_ = std::string("journal write: ") + std::strerror(errno);
            } else {
                off += std::size_t(n);
            }
        }
    }
    void sync(std::uint64_t written) {
        if (!error_ && durable_sync(fd_) != 0) error_ = std::string("journal fsync: ") + std::strerror(errno);
        if (!error_) marks_.durable->store(written, std::memory_order_release);
    }

    void io_loop() {
        std::uint64_t unsynced = 0, written = marks_.flushed->load();
        auto last_sync = std::chrono::steady_clock::now();
        auto idle = fsync_ ? fsync_->idle : std::chrono::nanoseconds(std::chrono::hours(1));
        for (;;) {
            Msg m{};
            if (unsynced > 0) {
                if (!to_io_.pop_for(m, idle)) {  // idle: group commit now
                    sync(written);
                    unsynced = 0;
                    last_sync = std::chrono::steady_clock::now();
                    continue;
                }
            } else {
                m = to_io_.pop();
            }
            bool stop = false;
            for (;;) {  // write everything queued, then decide on one fsync
                if (m.stop) { stop = true; break; }
                if (m.rotate_fd >= 0) {
                    if (unsynced > 0 && fsync_ && fsync_->mode != FsyncPolicy::Mode::Never) sync(written);
                    unsynced = 0;
                    ::close(fd_);
                    fd_ = m.rotate_fd;
                    if (!to_io_.try_pop(m)) break;
                    continue;
                }
                write_all(chunks_[m.chunk]);
                written = m.last;
                marks_.flushed->store(written, std::memory_order_release);
                unsynced += std::max<std::uint64_t>(m.records, 1);
                chunks_[m.chunk].clear();
                free_.push(m.chunk);
                if (!to_io_.try_pop(m)) break;
            }
            bool due = false;
            if (!fsync_ || fsync_->mode == FsyncPolicy::Mode::Never) {
                marks_.durable->store(written, std::memory_order_release);
                unsynced = 0;
            } else if (fsync_->mode == FsyncPolicy::Mode::EveryN) {
                due = unsynced >= fsync_->n;
            } else {
                due = std::chrono::steady_clock::now() - last_sync >= fsync_->interval;
            }
            if (due || (stop && unsynced > 0)) {
                sync(written);
                unsynced = 0;
                last_sync = std::chrono::steady_clock::now();
            }
            if (stop) return;
        }
    }

    int fd_;
    std::optional<FsyncPolicy> fsync_;
    Marks marks_;
    std::vector<std::string> chunks_;
    std::size_t cur_ = 0;
    std::uint64_t last_ = 0, records_ = 0;
    Queue<Msg> to_io_;
    Queue<std::size_t> free_;
    std::optional<std::string> error_;
    std::thread io_;
};

}  // namespace orderer
