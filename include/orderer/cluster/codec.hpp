// codec.hpp — binary encodings shared with orderer-rust (orderer-cluster
// codec.rs): protocol messages, WAL frame bodies and command-batch log entry
// payloads. Little-endian. Decoders never read out of bounds; they throw
// CodecError on malformed input.
#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <vector>

#include "../consensus.hpp"
#include "../journal.hpp"

namespace orderer::cluster {

using orderer::consensus::Disk;
using orderer::consensus::Effect;
using orderer::consensus::Bytes;
using orderer::consensus::Entry;
using orderer::consensus::Index;
using orderer::consensus::Msg;
using orderer::consensus::NodeId;
using orderer::consensus::Persist;
using orderer::consensus::Term;

struct CodecError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

inline constexpr std::size_t MAX_ENTRY = 16u << 20;
inline constexpr std::size_t CMD_BYTES = 32;
inline constexpr std::uint8_t TAG_BATCH = 1, TAG_SESSION_BATCH = 2;

class Reader {
public:
    Reader(const std::uint8_t* p, std::size_t n) : p_(p), n_(n) {}
    std::size_t remaining() const { return n_ - pos_; }
    const std::uint8_t* bytes(std::size_t n) {
        if (remaining() < n) throw CodecError("truncated input");
        const std::uint8_t* r = p_ + pos_;
        pos_ += n;
        return r;
    }
    std::uint8_t u8() { return *bytes(1); }
    std::uint32_t u32() { std::uint32_t v; std::memcpy(&v, bytes(4), 4); return v; }
    std::uint64_t u64() { std::uint64_t v; std::memcpy(&v, bytes(8), 8); return v; }
    void finish() const { if (remaining()) throw CodecError("trailing bytes"); }

private:
    const std::uint8_t* p_;
    std::size_t n_, pos_ = 0;
};

inline void put_u8(std::vector<std::uint8_t>& o, std::uint8_t v) { o.push_back(v); }
inline void put_u32(std::vector<std::uint8_t>& o, std::uint32_t v) { auto* p = reinterpret_cast<std::uint8_t*>(&v); o.insert(o.end(), p, p + 4); }
inline void put_u64(std::vector<std::uint8_t>& o, std::uint64_t v) { auto* p = reinterpret_cast<std::uint8_t*>(&v); o.insert(o.end(), p, p + 8); }

inline void put_entry(std::vector<std::uint8_t>& o, const Entry& e) {
    put_u32(o, e.term);
    put_u32(o, static_cast<std::uint32_t>(e.data.size()));
    o.insert(o.end(), e.data.begin(), e.data.end());
}
inline Entry get_entry(Reader& r) {
    Entry e;
    e.term = r.u32();
    const std::size_t n = r.u32();
    if (n > MAX_ENTRY) throw CodecError("entry too large");
    const auto* p = r.bytes(n);
    e.data = Bytes(std::vector<std::uint8_t>(p, p + n));
    return e;
}
inline void put_entries(std::vector<std::uint8_t>& o, const std::vector<Entry>& es) {
    put_u32(o, static_cast<std::uint32_t>(es.size()));
    for (const auto& e : es) put_entry(o, e);
}
inline std::vector<Entry> get_entries(Reader& r) {
    const std::size_t n = r.u32();
    // Each entry is at least 8 bytes; reject counts the input cannot hold.
    if (n > r.remaining() / 8) throw CodecError("entry count");
    std::vector<Entry> v;
    v.reserve(n);
    for (std::size_t i = 0; i < n; ++i) v.push_back(get_entry(r));
    return v;
}
inline bool get_flag(Reader& r) {
    const auto v = r.u8();
    if (v > 1) throw CodecError("flag");
    return v == 1;
}

inline void encode_msg(const Msg& m, std::vector<std::uint8_t>& o) {
    switch (m.kind) {
        case Msg::Kind::Vote:
            put_u8(o, 1); put_u32(o, m.term); put_u64(o, m.last_index); put_u32(o, m.last_term); put_u8(o, m.pre);
            break;
        case Msg::Kind::VoteReply:
            put_u8(o, 2); put_u32(o, m.term); put_u8(o, m.granted); put_u8(o, m.pre);
            break;
        case Msg::Kind::Append:
            put_u8(o, 3); put_u32(o, m.term); put_u64(o, m.prev_index); put_u32(o, m.prev_term); put_u64(o, m.commit);
            put_entries(o, m.entries);
            break;
        case Msg::Kind::AppendReply:
            put_u8(o, 4); put_u32(o, m.term); put_u8(o, m.success); put_u64(o, m.index);
            break;
    }
}

inline Msg decode_msg(Reader& r) {
    switch (r.u8()) {
        case 1: { Term t = r.u32(); Index li = r.u64(); Term lt = r.u32(); bool pre = get_flag(r); return Msg::vote(t, li, lt, pre); }
        case 2: { Term t = r.u32(); bool g = get_flag(r); bool pre = get_flag(r); return Msg::vote_reply(t, g, pre); }
        case 3: {
            Term t = r.u32(); Index pi = r.u64(); Term pt = r.u32(); Index c = r.u64();
            auto es = get_entries(r);
            return Msg::append(t, pi, pt, std::move(es), c);
        }
        case 4: { Term t = r.u32(); bool s = get_flag(r); Index i = r.u64(); return Msg::append_reply(t, s, i); }
        default: throw CodecError("message tag");
    }
}

// A WAL frame body: one atomic consensus write (the token is not stored).
inline void encode_persist(const Persist& p, std::vector<std::uint8_t>& o) {
    put_u32(o, p.term);
    put_u8(o, p.voted_for ? *p.voted_for : 255);
    if (p.compact) { put_u8(o, 1); put_u64(o, p.compact->first); put_u32(o, p.compact->second); }
    else put_u8(o, 0);
    put_u64(o, p.from);
    put_entries(o, p.entries);
}

inline Persist decode_persist(const std::uint8_t* d, std::size_t n) {
    Reader r(d, n);
    Persist p;
    p.term = r.u32();
    const auto v = r.u8();
    if (v != 255) p.voted_for = v;
    switch (r.u8()) {
        case 0: break;
        case 1: { Index i = r.u64(); Term t = r.u32(); p.compact = std::pair<Index, Term>{i, t}; break; }
        default: throw CodecError("compact flag");
    }
    p.from = r.u64();
    if (p.from == 0) throw CodecError("write starts at index 0");
    p.entries = get_entries(r);
    r.finish();
    return p;
}

/// Apply one write to a recovered disk image (same rules as the protocol
/// core's Disk::apply). Writes must be applied in issue order.
inline void apply_persist(Disk& d, const Persist& p) {
    d.term = p.term;
    d.voted_for = p.voted_for;
    if (p.compact) {
        const auto [i, t] = *p.compact;
        const std::size_t drop = i > d.base ? std::min<std::size_t>(i - d.base, d.log.size()) : 0;
        d.log.erase(d.log.begin(), d.log.begin() + static_cast<std::ptrdiff_t>(drop));
        if (i > d.base) { d.base = i; d.base_term = t; }
    }
    const std::size_t keep = p.from - 1 - d.base;
    if (keep < d.log.size()) d.log.resize(keep);
    for (const auto& e : p.entries) d.log.push_back(e);
}

// ---- command batches: the payload of a log entry ----

using CmdSym = std::pair<matcher::Symbol, matcher::Command>;

/// One command on the wire: the journal record's bytes 8..40 (symbol, cmd,
/// side, otype, tif, order_id, price, qty) without iseq, checksum or padding.
/// Written directly: building and sealing a full record per command costs more
/// than everything else in the batch.
inline void put_cmd32(std::uint8_t* r, matcher::Symbol sym, const matcher::Command& c) {
    std::memset(r, 0, CMD_BYTES);
    std::memcpy(r, &sym, 4);
    std::memcpy(r + 8, &c.order_id, 8);
    switch (c.kind) {
        case matcher::Command::Kind::New: {
            r[4] = 1;
            r[5] = c.side == matcher::Side::Ask ? 1 : 0;
            r[6] = c.otype == matcher::OType::Market ? 1 : 0;
            r[7] = orderer::tif_code(c.tif);
            const std::uint64_t price = std::uint64_t(c.price);
            std::memcpy(r + 16, &price, 8);
            std::memcpy(r + 24, &c.qty, 8);
            break;
        }
        case matcher::Command::Kind::Cancel: r[4] = 2; break;
        case matcher::Command::Kind::Replace: {
            r[4] = 3;
            const std::uint64_t price = std::uint64_t(c.price);
            std::memcpy(r + 16, &price, 8);
            std::memcpy(r + 24, &c.qty, 8);
            break;
        }
    }
}

/// `n` commands: 0x01 | n u32 | n * 32 bytes (a journal record minus iseq,
/// checksum and padding); the session form is 0x02 | client u64 | seq u64 | n | ...
inline void encode_batch(const CmdSym* cmds, std::size_t n, std::vector<std::uint8_t>& o,
                         std::optional<std::pair<std::uint64_t, std::uint64_t>> session = std::nullopt) {
    if (session) { put_u8(o, TAG_SESSION_BATCH); put_u64(o, session->first); put_u64(o, session->second); }
    else put_u8(o, TAG_BATCH);
    put_u32(o, static_cast<std::uint32_t>(n));
    const std::size_t at = o.size();
    o.resize(at + n * CMD_BYTES);
    for (std::size_t i = 0; i < n; ++i) put_cmd32(o.data() + at + i * CMD_BYTES, cmds[i].first, cmds[i].second);
}

/// The session a payload belongs to, if any, without decoding commands.
inline std::optional<std::pair<std::uint64_t, std::uint64_t>> session_of(const std::uint8_t* d, std::size_t n) {
    if (n < 17 || d[0] != TAG_SESSION_BATCH) return std::nullopt;
    std::uint64_t c, s;
    std::memcpy(&c, d + 1, 8);
    std::memcpy(&s, d + 9, 8);
    return std::pair{c, s};
}

/// Decode a plain or session-tagged payload. Empty = a leader's no-op.
inline std::vector<CmdSym> decode_batch(const std::uint8_t* d, std::size_t n) {
    std::vector<CmdSym> out;
    if (n == 0) return out;
    Reader r(d, n);
    switch (r.u8()) {
        case TAG_BATCH: break;
        case TAG_SESSION_BATCH: r.u64(); r.u64(); break;
        default: throw CodecError("entry tag");
    }
    const std::size_t count = r.u32();
    if (count != r.remaining() / CMD_BYTES || r.remaining() % CMD_BYTES != 0) throw CodecError("batch length");
    out.reserve(count);
    std::uint8_t rec[orderer::CMD_RECORD] = {0};
    for (std::size_t i = 0; i < count; ++i) {
        std::memcpy(rec + 8, r.bytes(CMD_BYTES), CMD_BYTES);
        auto c = orderer::decode_cmd(rec);
        if (!c) throw CodecError("command");
        out.emplace_back(c->sym, c->cmd);
    }
    return out;
}

}  // namespace orderer::cluster
