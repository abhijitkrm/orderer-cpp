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
inline constexpr std::size_t HEADER = 64, CMD_RECORD = 40, EVT_RECORD = 48;
inline std::size_t record_size(Kind k) { return k == Kind::Cmd ? CMD_RECORD : EVT_RECORD; }

/// spec/JOURNAL.md §1.
inline fs::path journal_path(const fs::path& dir, Kind k, std::uint32_t p, JournalFormat f) {
    return dir / (std::string(kind_name(k)) + "-" + std::to_string(p) +
                  (f == JournalFormat::Jsonl ? ".journal" : ".bin"));
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
    put_u16(h.data() + 4, 1);
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
    return h;
}

inline JournalHeader parse_binary_header(const fs::path& p, const std::string& bytes) {
    if (bytes.size() < HEADER || bytes.compare(0, 4, "ORDJ") != 0) corrupt(p, "bad magic");
    auto d = reinterpret_cast<const std::uint8_t*>(bytes.data());
    if (d[4] != 1 || d[5] != 0) corrupt(p, "unsupported version");
    JournalHeader h{};
    if (d[6] == 1) h.kind = Kind::Cmd;
    else if (d[6] == 2) h.kind = Kind::Evt;
    else corrupt(p, "bad kind");
    if (get_u32le(d + 16) != record_size(h.kind)) corrupt(p, "bad record_size");
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

/// Split text into lines; torn (no final newline) is corruption.
inline std::vector<std::string_view> jsonl_lines(const fs::path& p, const std::string& text) {
    if (!text.empty() && text.back() != '\n') corrupt(p, "torn tail (final line has no newline)");
    std::vector<std::string_view> out;
    std::size_t pos = 0;
    while (pos < text.size()) {
        auto e = text.find('\n', pos);
        out.push_back(std::string_view(text).substr(pos, e - pos));
        pos = e + 1;
    }
    return out;
}

inline std::pair<JournalHeader, std::vector<CmdRecord>> read_cmd_journal(const fs::path& p, JournalFormat f) {
    std::string bytes = read_file(p);
    JournalHeader h{};
    std::vector<CmdRecord> recs;
    if (f == JournalFormat::Binary) {
        h = parse_binary_header(p, bytes);
        std::size_t body = bytes.size() - HEADER;
        if (body % CMD_RECORD) corrupt(p, "torn tail (partial record)");
        auto d = reinterpret_cast<const std::uint8_t*>(bytes.data()) + HEADER;
        for (std::size_t i = 0; i < body / CMD_RECORD; ++i) {
            auto r = decode_cmd(d + i * CMD_RECORD);
            if (!r) corrupt(p, "record " + std::to_string(i) + ": bad codes");
            recs.push_back(*r);
        }
    } else {
        auto lines = jsonl_lines(p, bytes);
        h = parse_jsonl_header(p, lines.empty() ? std::string_view() : lines[0]);
        for (std::size_t i = 1; i < lines.size(); ++i) {
            auto l = lines[i];
            auto iseq = flat::get_u64(l, "iseq");
            auto sym = flat::get_u64(l, "symbol");
            auto cmd = flat::parse_command(l);
            if (!iseq || !sym || *sym > UINT32_MAX || !cmd)
                corrupt(p, "line " + std::to_string(i + 1) + ": malformed record: " + std::string(l));
            recs.push_back({*iseq, Symbol(*sym), *cmd});
        }
    }
    if (h.kind != Kind::Cmd) corrupt(p, "not a command journal");
    for (std::size_t i = 1; i < recs.size(); ++i)
        if (recs[i].iseq <= recs[i - 1].iseq)
            corrupt(p, "iseq not increasing (" + std::to_string(recs[i - 1].iseq) + " then " +
                           std::to_string(recs[i].iseq) + ")");
    return {h, recs};
}

/// Every partition's command journal in `dir` (count from cmd-0's header).
inline std::pair<JournalHeader, std::vector<std::vector<CmdRecord>>> read_cmd_dir(const fs::path& dir, JournalFormat f) {
    auto first = journal_path(dir, Kind::Cmd, 0, f);
    auto [h0, r0] = read_cmd_journal(first, f);
    if (h0.partition != 0) corrupt(first, "header partition is not 0");
    std::vector<std::vector<CmdRecord>> all;
    all.push_back(std::move(r0));
    for (std::uint32_t p = 1; p < h0.partitions; ++p) {
        auto path = journal_path(dir, Kind::Cmd, p, f);
        auto [h, r] = read_cmd_journal(path, f);
        if (h.partition != p || h.partitions != h0.partitions || !flat::same_book(h.book, h0.book))
            corrupt(path, "header does not match its file name, partition count or book config");
        all.push_back(std::move(r));
    }
    return {h0, std::move(all)};
}

/// An event journal as canonical symbol-tagged lines.
inline std::vector<std::string> read_evt_journal(const fs::path& p, JournalFormat f) {
    std::string bytes = read_file(p);
    std::vector<std::string> out;
    if (f == JournalFormat::Binary) {
        parse_binary_header(p, bytes);
        std::size_t body = bytes.size() - HEADER;
        if (body % EVT_RECORD) corrupt(p, "torn tail (partial record)");
        auto d = reinterpret_cast<const std::uint8_t*>(bytes.data()) + HEADER;
        for (std::size_t i = 0; i < body / EVT_RECORD; ++i) {
            auto r = decode_evt(d + i * EVT_RECORD);
            if (!r) corrupt(p, "record " + std::to_string(i) + ": bad codes");
            std::string l;
            write_canonical_sym(r->first, r->second.first, r->second.second, l);
            out.push_back(std::move(l));
        }
    } else {
        auto lines = jsonl_lines(p, bytes);
        parse_jsonl_header(p, lines.empty() ? std::string_view() : lines[0]);
        for (std::size_t i = 1; i < lines.size(); ++i) out.emplace_back(lines[i]);
    }
    return out;
}

// ---- writing -------------------------------------------------------------------------------

/// Create (header written) or open for append (header checked); fd at end.
inline int open_journal(const JournalConfig& cfg, Kind k, std::uint32_t p, std::uint32_t P, const BookConfig& book) {
    auto path = journal_path(cfg.dir, k, p, cfg.format);
    if (cfg.append && fs::exists(path)) {
        JournalHeader want{k, p, P, book};
        if (!(read_header(path, cfg.format) == want)) corrupt(path, "header does not match the pipeline");
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error(path.string() + ": " + std::strerror(errno));
        return fd;
    }
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) throw std::runtime_error(path.string() + ": " + std::strerror(errno));
    std::string h;
    if (cfg.format == JournalFormat::Jsonl) h = jsonl_header(k, p, P, book);
    else { auto b = binary_header(k, p, P, book); h.assign(reinterpret_cast<const char*>(b.data()), b.size()); }
    if (::write(fd, h.data(), h.size()) != ssize_t(h.size()))
        throw std::runtime_error(path.string() + ": header write failed");
    return fd;
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
        to_io_.push(Msg{cur_, last_, records_, false});
        records_ = 0;
        cur_ = free_.pop();
    }

    /// Write and (per policy) sync everything; returns the first I/O error.
    std::optional<std::string> finish() {
        if (io_.joinable()) {
            hand_off();
            to_io_.push(Msg{0, 0, 0, true});
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
