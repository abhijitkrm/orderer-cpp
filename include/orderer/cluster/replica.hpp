// replica.hpp — the replica runtime: one driver thread owns the protocol Node
// and executes its effects (WAL writes on an I/O thread, network sends, state
// machine applies on an applier thread, snapshots), as in orderer-rust
// (replica.rs; spec/REPLICATION.md 13). Acknowledgement is at commit; the
// state machine applies on its own thread so a slow one cannot starve
// heartbeats, and producers are pushed back with Busy when it falls behind.
#pragma once

#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "state_machines.hpp"
#include "transport.hpp"

namespace orderer::cluster {

struct ReplicaConfig {
    NodeId id = 0;
    std::vector<std::pair<NodeId, Addr>> members;
    std::uint64_t cluster_id = 1;
    std::filesystem::path dir;
    int tick_ms = 10;
    consensus::Config consensus{30, 3, 256, true};
    std::uint64_t compact_every = 0;  // 0 = never
    std::uint64_t digest_every = 0;   // 0 = off
    bool fsync = true;                // false: tests/benchmarks only; loses durability
    int propose_timeout_ms = 5000;
    std::size_t max_pending = 4096;
    std::uint64_t max_apply_backlog = 8192;
    std::optional<std::vector<std::uint8_t>> auth_key;  // pre-shared cluster key
};

struct ProposeResult {
    enum class Kind { Ok, NotLeader, Busy, Timeout, Superseded, Unknown, Stopped } kind = Kind::Ok;
    Index index = 0;
    std::optional<NodeId> hint;  // NotLeader: the leader this node last heard from
    bool ok() const { return kind == Kind::Ok; }
};

struct Status {
    NodeId id = 0;
    consensus::Role role = consensus::Role::Follower;
    Term term = 0;
    std::optional<NodeId> leader;
    Index commit = 0, applied = 0, base = 0, last_index = 0;
    std::size_t pending = 0;
    std::string halted;  // empty = running
    std::uint64_t digests_verified = 0;
};

class StartError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {
template <class T>
class Queue {
public:
    void push(T v) {
        { std::lock_guard g(m_); q_.push_back(std::move(v)); }
        cv_.notify_one();
    }
    /// Wait for an item until `deadline`.
    std::optional<T> pop_until(Clock::time_point deadline) {
        std::unique_lock g(m_);
        cv_.wait_until(g, deadline, [&] { return !q_.empty(); });
        return take();
    }
    std::optional<T> pop() {  // blocking
        std::unique_lock g(m_);
        cv_.wait(g, [&] { return !q_.empty(); });
        return take();
    }
    std::optional<T> try_pop() { std::lock_guard g(m_); return take(); }

private:
    std::optional<T> take() {
        if (q_.empty()) return std::nullopt;
        T v = std::move(q_.front());
        q_.pop_front();
        return v;
    }
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<T> q_;
};
}  // namespace detail

class Replica {
public:
    using HashResult = std::optional<std::pair<Index, Digest>>;

    static std::unique_ptr<Replica> start(ReplicaConfig cfg, std::unique_ptr<StateMachine> sm,
                                          std::shared_ptr<Filter> filter = std::make_shared<Filter>()) {
        return std::unique_ptr<Replica>(new Replica(std::move(cfg), std::move(sm), std::move(filter)));
    }

    ~Replica() { crash(); }

    /// Submit a pre-encoded log entry payload; blocks until committed.
    ProposeResult propose(std::vector<std::uint8_t> payload) { return propose_async(std::move(payload)).get(); }

    std::future<ProposeResult> propose_async(std::vector<std::uint8_t> payload) {
        auto pr = std::make_shared<std::promise<ProposeResult>>();
        auto fut = pr->get_future();
        if (stopped_ || done_) { pr->set_value({ProposeResult::Kind::Stopped, 0, std::nullopt}); return fut; }
        Event e;
        e.kind = Event::Kind::Propose;
        e.payload = std::move(payload);
        e.reply = pr;
        events_.push(std::move(e));
        if (done_) answer_stragglers();  // the driver ended while we queued
        return fut;
    }

    /// Linearizable read barrier: commits an empty entry and waits for the
    /// state machine to apply it. A deposed leader cannot commit, so it cannot answer.
    ProposeResult barrier() {
        ProposeResult r = propose({});
        if (!r.ok()) return r;
        const auto end = Clock::now() + std::chrono::seconds(30);
        while (sm_applied_.load(std::memory_order_acquire) < r.index) {
            if (Clock::now() > end) return {ProposeResult::Kind::Timeout, 0, std::nullopt};
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return r;
    }

    Status status() const { std::lock_guard g(status_m_); return status_; }

    /// (applied index, state digest), if the state machine supports digests.
    HashResult state_hash() {
        auto pr = std::make_shared<std::promise<HashResult>>();
        auto fut = pr->get_future();
        if (stopped_ || done_) return std::nullopt;
        Event e;
        e.kind = Event::Kind::Hash;
        e.hash_reply = pr;
        events_.push(std::move(e));
        if (done_) answer_stragglers();
        return fut.get();
    }

    /// Graceful stop: queued WAL writes are flushed first.
    void stop() { shutdown(false); }
    /// Simulated power cut: threads stop at once; writes not yet fsynced are discarded.
    void crash() { shutdown(true); }

    std::shared_ptr<Filter> filter;
    const ReplicaConfig& config() const { return cfg_; }

private:
    struct Event {
        enum class Kind { Inbound, PersistDone, Propose, Hash, SnapshotStored, DigestComputed, Fatal, Stop } kind = Kind::Stop;
        Inbound in;
        std::uint64_t token = 0;
        std::vector<std::uint8_t> payload;
        std::shared_ptr<std::promise<ProposeResult>> reply;
        std::shared_ptr<std::promise<HashResult>> hash_reply;
        Index index = 0;
        Digest digest{};
        std::string text;
    };
    struct Job {
        enum class Kind { Apply, Snapshot, Restore, Hash, Stop } kind = Kind::Stop;
        Index index = 0;
        Entry entry;
        Term term = 0;
        std::shared_ptr<std::promise<HashResult>> hash_reply;
    };
    struct Pending {
        Term term;
        std::shared_ptr<std::promise<ProposeResult>> reply;
        Clock::time_point deadline;
    };

    Replica(ReplicaConfig cfg, std::unique_ptr<StateMachine> sm, std::shared_ptr<Filter> flt)
        : filter(std::move(flt)), cfg_(std::move(cfg)), sm_(std::move(sm)) {
        ignore_sigpipe();
        Disk disk;
        Repair repair;
        try {
            wal_ = std::make_unique<Wal>(Wal::open(cfg_.dir / "wal", disk, repair));
        } catch (const WalError& e) {
            throw StartError(e.what());
        }
        snaps_ = std::make_shared<SnapStore>(cfg_.dir / "snap");
        if (disk.base > 0) {
            auto s = snaps_->get(disk.base);
            if (!s) throw StartError("log base " + std::to_string(disk.base) + " needs a snapshot that cannot be read");
            if (auto err = sm_->restore(disk.base, s->second.data(), s->second.size())) throw StartError("state machine: " + *err);
        }
        sm_applied_ = disk.base;
        std::vector<NodeId> peers;
        for (auto& m : cfg_.members) peers.push_back(m.first);
        const std::uint64_t seed =
            static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()) ^ cfg_.cluster_id ^ (std::uint64_t(cfg_.id) << 32);
        node_ = std::make_unique<consensus::Node>(cfg_.id, peers, cfg_.consensus, seed, std::move(disk));
        status_ = Status{cfg_.id, consensus::Role::Follower, node_->term(), std::nullopt, node_->commit_index(),
                         node_->applied_index(), node_->base(), node_->last_index(), 0, "", 0};

        transport_ = std::make_unique<Transport>(
            cfg_.id, cfg_.members, cfg_.cluster_id,
            [this](Inbound in) { Event e; e.kind = Event::Kind::Inbound; e.in = std::move(in); events_.push(std::move(e)); },
            snaps_, filter, cfg_.auth_key);

        wal_thread_ = std::thread([this] { wal_loop(); });
        applier_ = std::thread([this] { applier_loop(); });
        driver_ = std::thread([this] { run(); });
    }

    void shutdown(bool abort) {
        if (stopped_.exchange(true)) return;
        if (abort) abort_ = true;
        Event e;
        e.kind = Event::Kind::Stop;
        events_.push(std::move(e));
        if (driver_.joinable()) driver_.join();
    }

    // ---- WAL thread ----
    void wal_loop() {
        while (auto first = wal_q_.pop()) {
            if (!first->has_value()) break;  // sentinel
            if (abort_) return;
            std::vector<Persist> batch;
            batch.push_back(std::move(**first));
            bool quit = false;
            while (auto more = wal_q_.try_pop()) {
                if (!more->has_value()) { quit = true; break; }
                batch.push_back(std::move(**more));
            }
            bool compacted = false;
            try {
                for (auto& p : batch) { wal_->append(p); compacted |= p.compact.has_value(); }
                wal_->sync(cfg_.fsync);
                if (abort_) return;
                if (compacted) wal_->rewrite();
            } catch (const std::exception& e) {
                fatal(std::string("wal write failed: ") + e.what());
                return;
            }
            Event d;
            d.kind = Event::Kind::PersistDone;
            d.token = batch.back().token;
            events_.push(std::move(d));
            if (quit) break;
        }
    }

    // ---- applier thread ----
    void applier_loop() {
        while (auto job = jobs_.pop()) {
            switch (job->kind) {
                case Job::Kind::Apply: {
                    sm_->apply(job->index, job->entry);
                    sm_applied_.store(job->index, std::memory_order_release);
                    if (cfg_.digest_every > 0 && job->index % cfg_.digest_every == 0) {
                        if (auto d = sm_->digest(job->index)) {
                            Event e; e.kind = Event::Kind::DigestComputed; e.index = job->index; e.digest = *d;
                            events_.push(std::move(e));
                        }
                    }
                    break;
                }
                case Job::Kind::Snapshot: {
                    auto data = sm_->snapshot(job->index);
                    try {
                        snaps_->put(job->index, job->term, data.data(), data.size());
                        Event e; e.kind = Event::Kind::SnapshotStored; e.index = job->index;
                        events_.push(std::move(e));
                    } catch (const std::exception& ex) {
                        fatal("writing snapshot " + std::to_string(job->index) + ": " + ex.what());
                    }
                    break;
                }
                case Job::Kind::Restore: {
                    auto s = snaps_->get(job->index);
                    if (!s) { fatal("reading snapshot " + std::to_string(job->index)); break; }
                    if (auto err = sm_->restore(job->index, s->second.data(), s->second.size())) { fatal("restoring snapshot: " + *err); break; }
                    sm_applied_.store(job->index, std::memory_order_release);
                    break;
                }
                case Job::Kind::Hash: {
                    const Index at = sm_applied_.load(std::memory_order_acquire);
                    auto d = sm_->deep_digest(at);
                    job->hash_reply->set_value(d ? HashResult{{at, *d}} : std::nullopt);
                    break;
                }
                case Job::Kind::Stop: return;
            }
        }
    }

    /// Anything still queued that expects an answer gets one (the driver is gone).
    void answer_stragglers() {
        while (auto e = events_.try_pop()) {
            if (e->reply) e->reply->set_value({ProposeResult::Kind::Stopped, 0, std::nullopt});
            if (e->hash_reply) e->hash_reply->set_value(std::nullopt);
        }
    }

    void fatal(const std::string& why) {
        Event e;
        e.kind = Event::Kind::Fatal;
        e.text = why;
        events_.push(std::move(e));
    }

    // ---- driver ----
    void run() {
        auto next_tick = Clock::now() + std::chrono::milliseconds(cfg_.tick_ms);
        bool running = true;
        while (running) {
            bool replicate = false;
            if (auto ev = events_.pop_until(next_tick)) {
                running = handle(std::move(*ev), replicate);
                for (int i = 0; running && i < 4096; ++i) {
                    auto more = events_.try_pop();
                    if (!more) break;
                    running = handle(std::move(*more), replicate);
                }
            }
            const auto now = Clock::now();
            if (now >= next_tick) {
                node_->tick();
                next_tick = now + std::chrono::milliseconds(cfg_.tick_ms);
                expire(now);
            }
            if (replicate) node_->replicate();
            pump();
            maybe_compact();
            pump();
            publish_status();
            if (!halted_.empty()) break;
        }
        publish_status();
        for (auto& [i, p] : pending_) p.reply->set_value({ProposeResult::Kind::Unknown, 0, std::nullopt});
        pending_.clear();
        done_ = true;
        answer_stragglers();
        transport_->shutdown();
        Job stop;
        stop.kind = Job::Kind::Stop;
        jobs_.push(std::move(stop));
        if (applier_.joinable()) applier_.join();
        wal_q_.push(std::nullopt);
        if (wal_thread_.joinable()) wal_thread_.join();
    }

    bool handle(Event ev, bool& replicate) {
        switch (ev.kind) {
            case Event::Kind::Stop: return false;
            case Event::Kind::PersistDone: {
                node_->persisted(ev.token);
                for (auto it = compact_tokens_.begin(); it != compact_tokens_.end();) {
                    if (it->first <= ev.token) { snaps_->gc(it->second); it = compact_tokens_.erase(it); } else ++it;
                }
                break;
            }
            case Event::Kind::Inbound: {
                Inbound& in = ev.in;
                if (in.kind == Inbound::Kind::Msg) node_->step(in.from, std::move(in.msg));
                else if (in.kind == Inbound::Kind::Snapshot) {
                    std::fprintf(stderr, "orderer-cluster node %d: installing snapshot %llu from node %d\n", int(cfg_.id),
                                 static_cast<unsigned long long>(in.index), int(in.from));
                    node_->install_snapshot(in.from, in.term, in.index, in.last_term);
                }
                else { digests_[in.index][in.from] = in.digest; check_digests(in.index); }
                break;
            }
            case Event::Kind::Propose: {
                const std::uint64_t behind = node_->applied_index() - std::min(node_->applied_index(), sm_applied_.load(std::memory_order_acquire));
                if (pending_.size() >= cfg_.max_pending || behind > cfg_.max_apply_backlog) {
                    ev.reply->set_value({ProposeResult::Kind::Busy, 0, std::nullopt});
                } else if (auto idx = node_->propose(std::move(ev.payload))) {
                    pending_[*idx] = Pending{node_->term(), ev.reply, Clock::now() + std::chrono::milliseconds(cfg_.propose_timeout_ms)};
                    replicate = true;
                } else {
                    ev.reply->set_value({ProposeResult::Kind::NotLeader, 0, node_->leader()});
                }
                break;
            }
            case Event::Kind::Hash: {
                Job j; j.kind = Job::Kind::Hash; j.hash_reply = ev.hash_reply;
                jobs_.push(std::move(j));
                break;
            }
            case Event::Kind::SnapshotStored:
                snapshot_pending_ = false;
                if (ev.index > node_->base() && ev.index <= node_->applied_index()) node_->compact(ev.index);
                break;
            case Event::Kind::DigestComputed: {
                digests_[ev.index][cfg_.id] = ev.digest;
                for (auto& [id, a] : cfg_.members) if (id != cfg_.id) transport_->send_digest(id, ev.index, ev.digest);
                check_digests(ev.index);
                break;
            }
            case Event::Kind::Fatal: halt(ev.text); break;
        }
        return true;
    }

    void expire(Clock::time_point now) {
        for (auto it = pending_.begin(); it != pending_.end();) {
            if (it->second.deadline <= now) {
                it->second.reply->set_value({ProposeResult::Kind::Timeout, 0, std::nullopt});
                it = pending_.erase(it);
            } else ++it;
        }
    }

    void pump() {
        for (;;) {
            auto effects = node_->take_effects();
            if (effects.empty()) return;
            for (auto& e : effects) {
                switch (e.kind) {
                    case Effect::Kind::Send: transport_->send(e.to, std::move(e.msg)); break;
                    case Effect::Kind::Persist:
                        if (e.persist.compact) compact_tokens_[e.persist.token] = e.persist.compact->first;
                        wal_q_.push(std::move(e.persist));
                        break;
                    case Effect::Kind::Apply: committed(e.index, std::move(e.entry)); break;
                    case Effect::Kind::NeedSnapshot: transport_->send_snapshot(e.to, node_->term(), e.index, e.term); break;
                    case Effect::Kind::Restore: { Job j; j.kind = Job::Kind::Restore; j.index = e.index; jobs_.push(std::move(j)); break; }
                }
            }
        }
    }

    /// An entry committed: acknowledge its proposer, then queue it for the state machine.
    void committed(Index index, Entry entry) {
        auto it = pending_.find(index);
        if (it != pending_.end()) {
            it->second.reply->set_value(it->second.term == entry.term ? ProposeResult{ProposeResult::Kind::Ok, index, std::nullopt}
                                                                      : ProposeResult{ProposeResult::Kind::Superseded, 0, std::nullopt});
            pending_.erase(it);
        }
        Job j; j.kind = Job::Kind::Apply; j.index = index; j.entry = std::move(entry);
        jobs_.push(std::move(j));
    }

    /// Fail-stop if this node's state differs from the majority's.
    void check_digests(Index index) {
        auto it = digests_.find(index);
        if (it == digests_.end() || !it->second.count(cfg_.id)) return;
        const auto& seen = it->second;
        const Digest own = seen.at(cfg_.id);
        const std::size_t n = cfg_.members.size(), quorum = n / 2 + 1;
        std::map<Digest, std::size_t> groups;
        for (auto& [id, d] : seen) ++groups[d];
        bool other_majority = false;
        for (auto& [d, c] : groups) if (c >= quorum && d != own) other_majority = true;
        if (groups[own] >= quorum) {
            if (seen.size() == n) { ++digests_verified_; digests_.erase(it); }
        } else if (other_majority) {
            halt("state diverged from the majority at index " + std::to_string(index) + " (own digest " + hex(own) + ")");
        } else if (seen.size() == n) {
            halt("state digests at index " + std::to_string(index) + " have no majority");
        }
        if (cfg_.digest_every > 0) {
            const Index keep_from = index > cfg_.digest_every * 64 ? index - cfg_.digest_every * 64 : 0;
            digests_.erase(digests_.begin(), digests_.lower_bound(keep_from));
        }
    }

    void halt(const std::string& why) {
        std::fprintf(stderr, "orderer-cluster node %d: HALT: %s\n", int(cfg_.id), why.c_str());
        halted_ = why;
    }

    void maybe_compact() {
        if (cfg_.compact_every == 0 || !halted_.empty() || snapshot_pending_) return;
        const Index applied = node_->applied_index();
        if (applied - std::min(applied, node_->base()) < cfg_.compact_every) return;
        const consensus::Entry* e = node_->entry_at(applied);
        if (!e) return;
        // The applier snapshots after applying `applied`, then reports back;
        // only then does the log drop the prefix.
        snapshot_pending_ = true;
        Job j; j.kind = Job::Kind::Snapshot; j.index = applied; j.term = e->term;
        jobs_.push(std::move(j));
    }

    void publish_status() {
        std::lock_guard g(status_m_);
        status_.role = node_->role();
        status_.term = node_->term();
        status_.leader = node_->leader();
        status_.commit = node_->commit_index();
        status_.applied = sm_applied_.load(std::memory_order_acquire);
        status_.base = node_->base();
        status_.last_index = node_->last_index();
        status_.pending = pending_.size();
        status_.halted = halted_;
        status_.digests_verified = digests_verified_;
    }

    ReplicaConfig cfg_;
    std::unique_ptr<StateMachine> sm_;
    std::unique_ptr<Wal> wal_;
    std::shared_ptr<SnapStore> snaps_;
    std::unique_ptr<consensus::Node> node_;
    std::unique_ptr<Transport> transport_;

    detail::Queue<Event> events_;
    detail::Queue<std::optional<Persist>> wal_q_;  // nullopt = stop
    detail::Queue<Job> jobs_;
    std::thread driver_, wal_thread_, applier_;
    std::atomic<bool> stopped_{false}, abort_{false}, done_{false};
    std::atomic<Index> sm_applied_{0};

    // Driver-thread state.
    std::unordered_map<Index, Pending> pending_;
    std::map<std::uint64_t, Index> compact_tokens_;
    std::map<Index, std::map<NodeId, Digest>> digests_;
    std::uint64_t digests_verified_ = 0;
    std::string halted_;
    bool snapshot_pending_ = false;

    mutable std::mutex status_m_;
    Status status_;
};

}  // namespace orderer::cluster
