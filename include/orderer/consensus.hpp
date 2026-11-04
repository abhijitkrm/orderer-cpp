// consensus.hpp — the replication protocol core (spec/REPLICATION.md), a
// pure state machine: no I/O, no clock. The caller feeds inputs (tick, step,
// propose, persisted, compact, install_snapshot) and executes the effects it
// emits. A faithful port of orderer-rust's `orderer-consensus`; the shared
// conformance vectors in vectors/replication/ prove the two produce identical
// effects and state for the same inputs.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace orderer::consensus {

using NodeId = std::uint8_t;
using Term = std::uint32_t;
using Index = std::uint64_t;

// xorshift64* seeded through splitmix64, bit-identical to the Rust port.
class Rng {
public:
    explicit Rng(std::uint64_t seed) {
        std::uint64_t z = seed + 0x9E3779B97F4A7C15ULL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z ^= z >> 31;
        s_ = z == 0 ? 1 : z;
    }
    std::uint64_t next_u64() {
        std::uint64_t x = s_;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s_ = x;
        return x * 0x2545F4914F6CDD1DULL;
    }
    std::uint64_t below(std::uint64_t n) { return next_u64() % n; }

private:
    std::uint64_t s_;
};

/// Immutable, cheaply copyable bytes. A log entry's payload is copied several
/// times on its way through a replica (log, WAL write, one message per
/// follower); sharing the buffer turns each copy into a reference-count bump.
class Bytes {
public:
    Bytes() = default;
    Bytes(std::vector<std::uint8_t> v) : p_(std::make_shared<const std::vector<std::uint8_t>>(std::move(v))) {}
    Bytes(std::initializer_list<std::uint8_t> il) : Bytes(std::vector<std::uint8_t>(il)) {}
    const std::uint8_t* data() const { return p_ ? p_->data() : nullptr; }
    std::size_t size() const { return p_ ? p_->size() : 0; }
    bool empty() const { return size() == 0; }
    const std::uint8_t* begin() const { return data(); }
    const std::uint8_t* end() const { return data() + size(); }
    std::uint8_t operator[](std::size_t i) const { return (*p_)[i]; }
    /// A copy of the contents as a vector.
    std::vector<std::uint8_t> vec() const { return p_ ? *p_ : std::vector<std::uint8_t>{}; }
    operator std::vector<std::uint8_t>() const { return vec(); }
    bool operator==(const Bytes& o) const {
        if (p_ == o.p_) return true;
        return size() == o.size() && (size() == 0 || std::memcmp(data(), o.data(), size()) == 0);
    }

private:
    std::shared_ptr<const std::vector<std::uint8_t>> p_;
};

struct Entry {
    Term term = 0;
    Bytes data;
    bool operator==(const Entry&) const = default;
};

struct Msg {
    enum class Kind { Vote, VoteReply, Append, AppendReply } kind = Kind::Vote;
    Term term = 0;
    // Vote
    Index last_index = 0;
    Term last_term = 0;
    bool pre = false;
    // VoteReply
    bool granted = false;
    // Append
    Index prev_index = 0;
    Term prev_term = 0;
    std::vector<Entry> entries;
    Index commit = 0;
    // AppendReply
    bool success = false;
    Index index = 0;

    static Msg vote(Term t, Index li, Term lt, bool pre) {
        Msg m; m.kind = Kind::Vote; m.term = t; m.last_index = li; m.last_term = lt; m.pre = pre; return m;
    }
    static Msg vote_reply(Term t, bool granted, bool pre) {
        Msg m; m.kind = Kind::VoteReply; m.term = t; m.granted = granted; m.pre = pre; return m;
    }
    static Msg append(Term t, Index pi, Term pt, std::vector<Entry> es, Index commit) {
        Msg m; m.kind = Kind::Append; m.term = t; m.prev_index = pi; m.prev_term = pt;
        m.entries = std::move(es); m.commit = commit; return m;
    }
    static Msg append_reply(Term t, bool success, Index index) {
        Msg m; m.kind = Kind::AppendReply; m.term = t; m.success = success; m.index = index; return m;
    }
};

// One atomic disk write: set the hard state, optionally discard the log
// prefix covered by a snapshot, truncate the log to `from - 1`, then append.
struct Persist {
    std::uint64_t token = 0;
    Term term = 0;
    std::optional<NodeId> voted_for;
    Index from = 0;
    std::vector<Entry> entries;
    std::optional<std::pair<Index, Term>> compact;
};

struct Effect {
    enum class Kind { Send, Persist, Apply, NeedSnapshot, Restore } kind = Kind::Send;
    NodeId to = 0;      // Send, NeedSnapshot
    Msg msg;            // Send
    consensus::Persist persist;  // Persist
    Index index = 0;    // Apply, NeedSnapshot, Restore
    Entry entry;        // Apply
    Term term = 0;      // NeedSnapshot, Restore
};

struct Disk {
    Term term = 0;
    std::optional<NodeId> voted_for;
    std::vector<Entry> log;
    Index base = 0;
    Term base_term = 0;
};

struct Config {
    std::uint32_t election_ticks = 10;
    std::uint32_t heartbeat_ticks = 2;
    std::size_t max_batch = 64;
    bool pre_vote = true;
};

enum class Role { Follower, PreCandidate, Candidate, Leader };

inline const char* role_name(Role r) {
    switch (r) {
        case Role::Follower: return "Follower";
        case Role::PreCandidate: return "PreCandidate";
        case Role::Candidate: return "Candidate";
        case Role::Leader: return "Leader";
    }
    return "?";
}

class Node {
public:
    Node(NodeId id, std::vector<NodeId> peers, Config cfg, std::uint64_t seed, Disk disk = {})
        : id_(id), peers_(std::move(peers)), cfg_(cfg),
          rng_(seed ^ (static_cast<std::uint64_t>(id) << 56)),
          term_(disk.term), voted_for_(disk.voted_for), log_(std::move(disk.log)),
          base_(disk.base), base_term_(disk.base_term) {
        std::sort(peers_.begin(), peers_.end());
        const std::size_t n = peers_.size();
        commit_ = applied_ = base_;
        durable_ = base_ + log_.size();
        votes_.assign(n, false);
        next_index_.assign(n, 1);
        match_index_.assign(n, 0);
        heard_.assign(n, false);
        snap_wait_.assign(n, 0);
        reset_timeout();
    }

    // ---- observers ----
    NodeId id() const { return id_; }
    Role role() const { return role_; }
    Term term() const { return term_; }
    std::optional<NodeId> leader() const { return leader_; }
    Index commit_index() const { return commit_; }
    Index applied_index() const { return applied_; }
    Index base() const { return base_; }
    Index last_index() const { return base_ + log_.size(); }
    const Entry* entry_at(Index i) const {
        if (i > base_ && i - base_ - 1 < log_.size()) return &log_[i - base_ - 1];
        return nullptr;
    }
    const char* fault() const { return fault_; }
    std::vector<Effect> take_effects() { return std::exchange(out_, {}); }

    // ---- inputs ----
    void tick() {
        for (auto& w : snap_wait_) w = w > 0 ? w - 1 : 0;
        if (role_ == Role::Leader) {
            ++heartbeat_elapsed_;
            if (heartbeat_elapsed_ >= cfg_.heartbeat_ticks) {
                heartbeat_elapsed_ = 0;
                broadcast_append();
            }
            ++election_elapsed_;
            if (election_elapsed_ >= election_timeout_) {
                // Check-quorum: step down without contact from a majority.
                const std::size_t me = pos(id_);
                std::size_t heard = 0;
                for (std::size_t i = 0; i < heard_.size(); ++i)
                    if (heard_[i] || i == me) ++heard;
                if (heard < quorum()) {
                    role_ = Role::Follower;
                    leader_.reset();
                }
                std::fill(heard_.begin(), heard_.end(), false);
                reset_timeout();
            }
            return;
        }
        ++election_elapsed_;
        if (election_elapsed_ >= election_timeout_) start_election();
    }

    // Append client data. Only the leader accepts; returns the entry's index
    // or nullopt (use leader() for a redirect hint). Call replicate() after.
    std::optional<Index> propose(std::vector<std::uint8_t> data) {
        if (role_ != Role::Leader) return std::nullopt;
        leader_append(Entry{term_, std::move(data)});
        return last_index();
    }

    void replicate() {
        if (role_ == Role::Leader) broadcast_append();
    }

    // The host has stored a snapshot of the state after applying `index`.
    void compact(Index index) {
        if (index <= base_ || index > applied_) { fault_ = "compact outside (base, applied]"; return; }
        const Term t = term_at(index);
        log_.erase(log_.begin(), log_.begin() + static_cast<std::ptrdiff_t>(index - base_));
        base_ = index;
        base_term_ = t;
        persist_with(last_index() + 1, {}, std::pair<Index, Term>{index, t});
    }

    // The host received and durably stored the leader's snapshot.
    void install_snapshot(NodeId from, Term term, Index index, Term last_term) {
        if (term < term_) return;
        become_follower(term, from);
        const Term reply_term = term_;
        if (index <= commit_) {
            send_gated(from, Msg::append_reply(reply_term, true, commit_));
            return;
        }
        if (index <= last_index() && term_at(index) == last_term) {
            log_.erase(log_.begin(), log_.begin() + static_cast<std::ptrdiff_t>(index - base_));
        } else {
            log_.clear();
            durable_ = index;
            ++epoch_;
        }
        base_ = index;
        base_term_ = last_term;
        commit_ = applied_ = index;
        persist_with(last_index() + 1, {}, std::pair<Index, Term>{index, last_term});
        Effect e; e.kind = Effect::Kind::Restore; e.index = index; e.term = last_term;
        out_.push_back(std::move(e));
        send_gated(from, Msg::append_reply(reply_term, true, index));
    }

    // The disk finished the write with `token` (and all earlier ones).
    void persisted(std::uint64_t token) {
        while (!writes_.empty() && writes_.front().token <= token) {
            Write w = std::move(writes_.front());
            writes_.pop_front();
            if (w.epoch == epoch_) durable_ = std::min(std::max(durable_, w.upto), last_index());
            for (auto& e : w.gated) out_.push_back(std::move(e));
        }
        if (role_ == Role::Leader) try_commit();
    }

    void step(NodeId from, Msg msg) {
        switch (msg.kind) {
            case Msg::Kind::Vote: on_vote(from, msg.term, msg.last_index, msg.last_term, msg.pre); break;
            case Msg::Kind::VoteReply: on_vote_reply(from, msg.term, msg.granted, msg.pre); break;
            case Msg::Kind::Append:
                on_append(from, msg.term, msg.prev_index, msg.prev_term, std::move(msg.entries), msg.commit);
                break;
            case Msg::Kind::AppendReply: on_append_reply(from, msg.term, msg.success, msg.index); break;
        }
    }

private:
    struct Write {
        std::uint64_t token;
        Index upto;
        std::uint64_t epoch;
        std::vector<Effect> gated;
    };

    std::size_t pos(NodeId id) const {
        return static_cast<std::size_t>(std::find(peers_.begin(), peers_.end(), id) - peers_.begin());
    }
    std::size_t quorum() const { return peers_.size() / 2 + 1; }
    Term last_term() const { return log_.empty() ? base_term_ : log_.back().term; }
    Term term_at(Index i) const {
        if (i == base_) return base_term_;
        const Entry* e = entry_at(i);
        return e ? e->term : 0;
    }
    void reset_timeout() {
        election_elapsed_ = 0;
        const std::uint64_t t = cfg_.election_ticks;
        election_timeout_ = static_cast<std::uint32_t>(t + rng_.below(t));
    }
    std::vector<NodeId> others() const {
        std::vector<NodeId> v;
        for (NodeId p : peers_) if (p != id_) v.push_back(p);
        return v;
    }
    void send(NodeId to, Msg m) {
        Effect e; e.kind = Effect::Kind::Send; e.to = to; e.msg = std::move(m);
        out_.push_back(std::move(e));
    }
    void send_gated(NodeId to, Msg m) {
        Effect e; e.kind = Effect::Kind::Send; e.to = to; e.msg = std::move(m);
        if (!writes_.empty()) writes_.back().gated.push_back(std::move(e));
        else out_.push_back(std::move(e));
    }
    void persist(Index from, std::vector<Entry> entries) { persist_with(from, std::move(entries), std::nullopt); }
    void persist_with(Index from, std::vector<Entry> entries, std::optional<std::pair<Index, Term>> compact) {
        const std::uint64_t token = next_token_++;
        const Index upto = from - 1 + entries.size();
        writes_.push_back(Write{token, upto, epoch_, {}});
        Effect e; e.kind = Effect::Kind::Persist;
        e.persist.token = token; e.persist.term = term_; e.persist.voted_for = voted_for_;
        e.persist.from = from; e.persist.entries = std::move(entries); e.persist.compact = compact;
        out_.push_back(std::move(e));
    }
    void persist_hard_state() { persist(last_index() + 1, {}); }
    void truncate_from(Index index) {
        if (index <= commit_) fault_ = "truncating a committed entry";
        log_.resize(index - base_ - 1);
        durable_ = std::min(durable_, index - 1);
        ++epoch_;
    }
    void become_follower(Term term, std::optional<NodeId> leader) {
        if (term > term_) {
            term_ = term;
            voted_for_.reset();
            persist_hard_state();
        }
        role_ = Role::Follower;
        leader_ = leader;
        reset_timeout();
    }
    void apply_committed() {
        while (applied_ < commit_) {
            ++applied_;
            Effect e; e.kind = Effect::Kind::Apply; e.index = applied_; e.entry = *entry_at(applied_);
            out_.push_back(std::move(e));
        }
    }

    // ---- elections ----
    void start_election() {
        if (cfg_.pre_vote && peers_.size() > 1) {
            role_ = Role::PreCandidate;
            leader_.reset();
            std::fill(votes_.begin(), votes_.end(), false);
            votes_[pos(id_)] = true;
            reset_timeout();
            for (NodeId p : others()) send(p, Msg::vote(term_ + 1, last_index(), last_term(), true));
        } else {
            campaign();
        }
    }
    std::size_t vote_count() const { return static_cast<std::size_t>(std::count(votes_.begin(), votes_.end(), true)); }
    void campaign() {
        ++term_;
        voted_for_ = id_;
        role_ = Role::Candidate;
        leader_.reset();
        std::fill(votes_.begin(), votes_.end(), false);
        votes_[pos(id_)] = true;
        persist_hard_state();
        reset_timeout();
        for (NodeId p : others()) send_gated(p, Msg::vote(term_, last_index(), last_term(), false));
        if (vote_count() >= quorum()) become_leader();
    }
    void become_leader() {
        role_ = Role::Leader;
        leader_ = id_;
        const Index next = last_index() + 1;
        std::fill(next_index_.begin(), next_index_.end(), next);
        std::fill(match_index_.begin(), match_index_.end(), 0);
        std::fill(heard_.begin(), heard_.end(), false);
        heartbeat_elapsed_ = 0;
        election_elapsed_ = 0;
        leader_append(Entry{term_, {}});  // the term's no-op
        broadcast_append();
    }
    bool up_to_date(Index li, Term lt) const {
        return std::make_pair(lt, li) >= std::make_pair(last_term(), last_index());
    }
    void on_vote(NodeId from, Term term, Index last_idx, Term last_t, bool pre) {
        const bool utd = up_to_date(last_idx, last_t);
        if (pre) {
            const bool live_leader = role_ == Role::Leader || (leader_.has_value() && election_elapsed_ < election_timeout_);
            const bool granted = term > term_ && utd && !live_leader;
            send(from, Msg::vote_reply(granted ? term : term_, granted, true));
            return;
        }
        if (term < term_) { send(from, Msg::vote_reply(term_, false, false)); return; }
        if (term > term_) become_follower(term, std::nullopt);
        const bool free = !voted_for_.has_value() || voted_for_ == from;
        const bool granted = free && utd;
        if (granted) {
            if (voted_for_ != from) { voted_for_ = from; persist_hard_state(); }
            reset_timeout();
        }
        send_gated(from, Msg::vote_reply(term_, granted, false));
    }
    void on_vote_reply(NodeId from, Term term, bool granted, bool pre) {
        if (pre) {
            if (role_ != Role::PreCandidate) return;
            if (!granted) { if (term > term_) become_follower(term, std::nullopt); return; }
            if (term != term_ + 1) return;
            votes_[pos(from)] = true;
            if (vote_count() >= quorum()) campaign();
            return;
        }
        if (term > term_) { become_follower(term, std::nullopt); return; }
        if (role_ != Role::Candidate || term < term_ || !granted) return;
        votes_[pos(from)] = true;
        if (vote_count() >= quorum()) become_leader();
    }

    // ---- replication ----
    void leader_append(Entry e) {
        log_.push_back(e);
        const Index idx = last_index();
        persist(idx, std::vector<Entry>{std::move(e)});
    }
    void broadcast_append() { for (NodeId p : others()) send_append(p); }
    void send_append(NodeId to) {
        const std::size_t i = pos(to);
        const Index next = std::max<Index>(next_index_[i], 1);
        if (next <= base_) {
            if (snap_wait_[i] == 0) {
                snap_wait_[i] = cfg_.election_ticks * 2;
                Effect e; e.kind = Effect::Kind::NeedSnapshot; e.to = to; e.index = base_; e.term = base_term_;
                out_.push_back(std::move(e));
            }
            return;
        }
        const Index prev_index = next - 1;
        const std::size_t start = prev_index - base_;
        const std::size_t end = std::min(log_.size(), start + cfg_.max_batch);
        std::vector<Entry> es(log_.begin() + static_cast<std::ptrdiff_t>(start), log_.begin() + static_cast<std::ptrdiff_t>(end));
        const Index sent = es.size();
        Msg m = Msg::append(term_, prev_index, term_at(prev_index), std::move(es), commit_);
        next_index_[i] = next + sent;
        send(to, std::move(m));
    }
    void on_append(NodeId from, Term term, Index prev_index, Term prev_term, std::vector<Entry> entries, Index leader_commit) {
        if (term < term_) { send(from, Msg::append_reply(term_, false, 0)); return; }
        become_follower(term, from);
        if (prev_index < base_) {
            const std::size_t skip = std::min<std::size_t>(base_ - prev_index, entries.size());
            entries.erase(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(skip));
            prev_index += skip;
            if (prev_index < base_) { send_gated(from, Msg::append_reply(term_, true, base_)); return; }
            prev_term = base_term_;
        }
        if (prev_index > last_index() || term_at(prev_index) != prev_term) {
            const Index hint = std::min<Index>(last_index(), prev_index > 0 ? prev_index - 1 : 0);
            send_gated(from, Msg::append_reply(term_, false, hint));
            return;
        }
        Index idx = prev_index;
        std::optional<std::size_t> first_new;
        for (std::size_t k = 0; k < entries.size(); ++k) {
            ++idx;
            if (idx <= last_index()) {
                if (term_at(idx) != entries[k].term) { truncate_from(idx); first_new = k; break; }
            } else {
                first_new = k;
                break;
            }
        }
        if (first_new) {
            const Index from_index = prev_index + 1 + *first_new;
            std::vector<Entry> fresh(entries.begin() + static_cast<std::ptrdiff_t>(*first_new), entries.end());
            for (const auto& e : fresh) log_.push_back(e);
            persist(from_index, std::move(fresh));
        }
        const Index matched = prev_index + entries.size();
        if (leader_commit > commit_) { commit_ = std::min(leader_commit, matched); apply_committed(); }
        send_gated(from, Msg::append_reply(term_, true, matched));
    }
    void on_append_reply(NodeId from, Term term, bool success, Index index) {
        if (term > term_) { become_follower(term, std::nullopt); return; }
        if (role_ != Role::Leader || term < term_) return;
        const std::size_t i = pos(from);
        heard_[i] = true;
        if (success) {
            match_index_[i] = std::max(match_index_[i], index);
            next_index_[i] = std::max(next_index_[i], index + 1);
            try_commit();
            if (next_index_[i] <= last_index()) send_append(from);
        } else {
            next_index_[i] = std::max<Index>(std::min(next_index_[i] - 1, index + 1), 1);
            send_append(from);
        }
    }
    void try_commit() {
        const std::size_t me = pos(id_);
        std::vector<Index> m(peers_.size());
        for (std::size_t i = 0; i < m.size(); ++i) m[i] = i == me ? durable_ : match_index_[i];
        std::sort(m.begin(), m.end(), std::greater<>());
        const Index idx = m[quorum() - 1];
        // Only current-term entries are committed by counting (Raft figure 8).
        if (idx > commit_ && term_at(idx) == term_) {
            commit_ = idx;
            apply_committed();
        }
    }

    NodeId id_;
    std::vector<NodeId> peers_;
    Config cfg_;
    Rng rng_;
    Term term_;
    std::optional<NodeId> voted_for_;
    std::vector<Entry> log_;
    Index base_;
    Term base_term_;
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    Index commit_ = 0, applied_ = 0, durable_ = 0;
    std::uint64_t epoch_ = 0;
    std::deque<Write> writes_;
    std::uint64_t next_token_ = 1;
    std::vector<bool> votes_;
    std::vector<Index> next_index_, match_index_;
    std::vector<bool> heard_;
    std::vector<std::uint32_t> snap_wait_;
    std::uint32_t election_elapsed_ = 0, election_timeout_ = 0, heartbeat_elapsed_ = 0;
    std::vector<Effect> out_;
    const char* fault_ = nullptr;
};

}  // namespace orderer::consensus
