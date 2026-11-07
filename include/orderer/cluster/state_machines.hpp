// state_machines.hpp — the StateMachine interface the log drives, plus the
// language-neutral RecorderSm and the exactly-once DedupSm wrapper (ports of
// orderer-rust's recorder.rs and dedup.rs).
#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "codec.hpp"
#include "sha256.hpp"

namespace orderer::cluster {

/// The deterministic state machine the replicated log drives. Called only
/// from the replica's applier thread, in log order.
class StateMachine {
public:
    virtual ~StateMachine() = default;
    /// Apply committed entry `index`. No-op entries arrive with empty data.
    virtual void apply(Index index, const Entry& entry) = 0;
    /// Serialize the state after applying `index`.
    virtual std::vector<std::uint8_t> snapshot(Index index) = 0;
    /// Replace the state with a snapshot taken at `index`; an error string on failure.
    virtual std::optional<std::string> restore(Index index, const std::uint8_t* data, std::size_t n) = 0;
    /// A digest of the state, for divergence detection; nullopt = unsupported.
    virtual std::optional<Digest> digest(Index) { return std::nullopt; }
    /// A digest of the whole state, however expensive, for on-demand checks
    /// (the client protocol's state hash). Defaults to digest().
    virtual std::optional<Digest> deep_digest(Index i) { return digest(i); }
};

/// Records every non-empty entry. Snapshot = the list `index u64 | len u32 |
/// data`; digest = SHA-256 of that list. Identical to orderer-rust's RecorderSm.
class RecorderSm : public StateMachine {
public:
    void apply(Index index, const Entry& e) override {
        if (!e.data.empty()) applied_.emplace_back(index, e.data);
    }
    std::vector<std::uint8_t> snapshot(Index) override { return serialize(); }
    std::optional<std::string> restore(Index, const std::uint8_t* d, std::size_t n) override {
        std::vector<std::pair<Index, std::vector<std::uint8_t>>> v;
        std::size_t at = 0;
        while (at < n) {
            if (n - at < 12) return "short recorder snapshot";
            Index i;
            std::uint32_t len;
            std::memcpy(&i, d + at, 8);
            std::memcpy(&len, d + at + 8, 4);
            if (at + 12 + len > n) return "short recorder snapshot";
            v.emplace_back(i, std::vector<std::uint8_t>(d + at + 12, d + at + 12 + len));
            at += 12 + len;
        }
        applied_ = std::move(v);
        return std::nullopt;
    }
    std::optional<Digest> digest(Index) override { auto b = serialize(); return sha256(b); }
    const std::vector<std::pair<Index, std::vector<std::uint8_t>>>& applied() const { return applied_; }

private:
    std::vector<std::uint8_t> serialize() const {
        std::vector<std::uint8_t> out;
        for (auto& [i, d] : applied_) { put_u64(out, i); put_u32(out, static_cast<std::uint32_t>(d.size())); out.insert(out.end(), d.begin(), d.end()); }
        return out;
    }
    std::vector<std::pair<Index, std::vector<std::uint8_t>>> applied_;
};

/// Applies a session's batch only when `seq` is newer than the last applied
/// for that client, so a retry that landed in the log twice takes effect once.
/// The table is part of every snapshot and digest.
class DedupSm : public StateMachine {
public:
    explicit DedupSm(std::unique_ptr<StateMachine> inner) : inner_(std::move(inner)) {}

    void apply(Index index, const Entry& e) override {
        if (auto s = session_of(e.data.data(), e.data.size())) {
            auto& last = last_[s->first];
            if (s->second <= last) { ++duplicates_; return; }
            last = s->second;
        }
        inner_->apply(index, e);
    }
    std::vector<std::uint8_t> snapshot(Index index) override {
        auto out = table_bytes();
        auto in = inner_->snapshot(index);
        put_u64(out, in.size());
        out.insert(out.end(), in.begin(), in.end());
        return out;
    }
    std::optional<std::string> restore(Index index, const std::uint8_t* d, std::size_t n) override {
        if (n < 4) return "short dedup snapshot";
        std::uint32_t count;
        std::memcpy(&count, d, 4);
        const std::size_t table_end = 4 + static_cast<std::size_t>(count) * 16;
        if (n < table_end + 8) return "short dedup snapshot";
        std::map<std::uint64_t, std::uint64_t> last;
        for (std::uint32_t i = 0; i < count; ++i) {
            std::uint64_t c, s;
            std::memcpy(&c, d + 4 + i * 16, 8);
            std::memcpy(&s, d + 12 + i * 16, 8);
            last[c] = s;
        }
        std::uint64_t len;
        std::memcpy(&len, d + table_end, 8);
        if (table_end + 8 + len > n) return "short dedup snapshot";
        if (auto err = inner_->restore(index, d + table_end + 8, len)) return err;
        last_ = std::move(last);
        return std::nullopt;
    }
    std::optional<Digest> digest(Index index) override {
        auto inner = inner_->digest(index);
        if (!inner) return std::nullopt;
        auto b = table_bytes();
        b.insert(b.end(), inner->begin(), inner->end());
        return sha256(b);
    }
    std::optional<Digest> deep_digest(Index index) override {
        auto inner = inner_->deep_digest(index);
        if (!inner) return std::nullopt;
        auto b = table_bytes();
        b.insert(b.end(), inner->begin(), inner->end());
        return sha256(b);
    }
    std::uint64_t duplicates() const { return duplicates_; }

private:
    std::vector<std::uint8_t> table_bytes() const {
        std::vector<std::uint8_t> out;
        put_u32(out, static_cast<std::uint32_t>(last_.size()));
        for (auto& [c, s] : last_) { put_u64(out, c); put_u64(out, s); }
        return out;
    }
    std::unique_ptr<StateMachine> inner_;
    std::map<std::uint64_t, std::uint64_t> last_;
    std::uint64_t duplicates_ = 0;
};

}  // namespace orderer::cluster
