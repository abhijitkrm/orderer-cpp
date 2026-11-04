// End-to-end: real replicas over TCP with real files (mirrors orderer-rust's
// orderer-cluster/tests).
#include <set>
#include <sstream>

#include "check.hpp"
#include "orderer/cluster/client.hpp"

using namespace orderer::cluster;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::vector<std::uint8_t> payload(std::uint64_t i) {
    std::vector<std::uint8_t> v(8);
    std::memcpy(v.data(), &i, 8);
    return v;
}
std::uint64_t id_of(const std::vector<std::uint8_t>& p) { std::uint64_t i; std::memcpy(&i, p.data(), 8); return i; }

/// A RecorderSm whose state the test can read while the applier thread runs.
struct Shared {
    std::mutex m;
    RecorderSm sm;
    std::optional<Index> corrupt_at;
    std::atomic<int> apply_delay_ms{0};
    std::vector<std::vector<std::uint8_t>> payloads() {
        std::lock_guard g(m);
        std::vector<std::vector<std::uint8_t>> v;
        for (auto& [i, d] : sm.applied()) v.push_back(d);
        return v;
    }
    std::size_t count() { std::lock_guard g(m); return sm.applied().size(); }
};

class Probe : public StateMachine {
public:
    explicit Probe(std::shared_ptr<Shared> s) : s_(std::move(s)) {}
    void apply(Index i, const Entry& e) override {
        if (int d = s_->apply_delay_ms.load()) std::this_thread::sleep_for(std::chrono::milliseconds(d));
        std::lock_guard g(s_->m);
        Entry c = e;
        if (s_->corrupt_at == i && !c.data.empty()) c.data[0] ^= 0xff;
        s_->sm.apply(i, c);
    }
    std::vector<std::uint8_t> snapshot(Index i) override { std::lock_guard g(s_->m); return s_->sm.snapshot(i); }
    std::optional<std::string> restore(Index i, const std::uint8_t* d, std::size_t n) override { std::lock_guard g(s_->m); return s_->sm.restore(i, d, n); }
    std::optional<Digest> digest(Index i) override { std::lock_guard g(s_->m); return s_->sm.digest(i); }

private:
    std::shared_ptr<Shared> s_;
};

std::vector<Addr> free_addrs(std::size_t n) {
    std::vector<Fd> keep;
    std::vector<Addr> out;
    for (std::size_t i = 0; i < n; ++i) {
        Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        ::inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        ::bind(fd.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa);
        socklen_t l = sizeof sa;
        ::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&sa), &l);
        Addr a; a.sa = sa; out.push_back(a);
        keep.push_back(std::move(fd));
    }
    return out;
}

struct Cluster {
    fs::path dir;
    std::vector<std::pair<NodeId, Addr>> members;
    std::vector<std::unique_ptr<Replica>> nodes;
    std::vector<std::shared_ptr<Shared>> shared;
    std::function<void(ReplicaConfig&)> tune;
    bool dedup = false;

    Cluster(const std::string& name, std::size_t n, std::function<void(ReplicaConfig&)> t = {}, bool dedup_ = false)
        : tune(std::move(t)), dedup(dedup_) {
        dir = fs::temp_directory_path() / ("orderer-cpp-e2e-" + name + "-" + std::to_string(::getpid()));
        fs::remove_all(dir);
        auto addrs = free_addrs(n);
        for (std::size_t i = 0; i < n; ++i) {
            members.emplace_back(static_cast<NodeId>(i), addrs[i]);
            nodes.emplace_back();
            shared.push_back(std::make_shared<Shared>());
        }
        for (std::size_t i = 0; i < n; ++i) start(i);
    }
    ~Cluster() {
        for (auto& n : nodes) n.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    ReplicaConfig config(std::size_t i) const {
        ReplicaConfig c;
        c.id = static_cast<NodeId>(i);
        c.members = members;
        c.dir = dir / ("n" + std::to_string(i));
        c.tick_ms = 5;
        c.consensus = orderer::consensus::Config{20, 2, 256, true};
        c.fsync = false;
        c.propose_timeout_ms = 3000;
        if (tune) tune(c);
        return c;
    }

    void start(std::size_t i) {
        std::unique_ptr<StateMachine> sm = std::make_unique<Probe>(shared[i]);
        if (dedup) sm = std::make_unique<DedupSm>(std::move(sm));
        nodes[i] = Replica::start(config(i), std::move(sm));
    }
    void crash(std::size_t i) { nodes[i].reset(); }
    void stop(std::size_t i) { nodes[i]->stop(); nodes[i].reset(); }
    void restart(std::size_t i) { shared[i] = std::make_shared<Shared>(); start(i); }

    int leader() const {
        int best = -1;
        Term bt = 0;
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (!nodes[i]) continue;
            auto s = nodes[i]->status();
            if (s.role == orderer::consensus::Role::Leader && (best < 0 || s.term > bt)) { best = int(i); bt = s.term; }
        }
        return best;
    }
    std::size_t wait_leader(std::chrono::milliseconds within = 5s) const {
        const auto end = Clock::now() + within;
        for (;;) {
            if (int l = leader(); l >= 0) return std::size_t(l);
            CHECK(Clock::now() < end) << "no leader within " << within.count() << "ms";
            if (Clock::now() >= end) return 0;
            std::this_thread::sleep_for(10ms);
        }
    }

    /// Propose via whichever node leads, retrying redirects. Timeout/Unknown are returned.
    ProposeResult propose(const std::vector<std::uint8_t>& p) {
        const auto end = Clock::now() + 10s;
        for (;;) {
            int l = leader();
            if (l < 0) { if (Clock::now() > end) return ProposeResult{ProposeResult::Kind::Stopped, 0, std::nullopt}; std::this_thread::sleep_for(10ms); continue; }
            auto r = nodes[l]->propose(p);
            if (r.kind == ProposeResult::Kind::NotLeader || r.kind == ProposeResult::Kind::Busy || r.kind == ProposeResult::Kind::Stopped) {
                if (Clock::now() > end) return r;
                std::this_thread::sleep_for(5ms);
                continue;
            }
            return r;
        }
    }

    void wait_applied(const std::vector<std::size_t>& who, std::size_t at_least, std::chrono::milliseconds within = 10s) {
        const auto end = Clock::now() + within;
        for (;;) {
            bool ok = true;
            for (auto i : who) ok = ok && shared[i]->count() >= at_least;
            if (ok) return;
            if (Clock::now() > end) { CHECK(false) << "nodes did not apply " << at_least; return; }
            std::this_thread::sleep_for(10ms);
        }
    }

    /// Wait until every running node has applied the same, stable sequence.
    std::vector<std::vector<std::uint8_t>> wait_converged(std::chrono::milliseconds within = 15s) {
        const auto end = Clock::now() + within;
        auto snap = [&] {
            std::vector<std::vector<std::vector<std::uint8_t>>> v;
            for (std::size_t i = 0; i < nodes.size(); ++i) if (nodes[i]) v.push_back(shared[i]->payloads());
            return v;
        };
        auto same = [](const auto& v) { for (auto& x : v) if (x != v[0]) return false; return true; };
        for (;;) {
            auto a = snap();
            if (same(a)) {
                std::this_thread::sleep_for(100ms);
                if (snap() == a) return a.empty() ? decltype(a)::value_type{} : a[0];
            }
            if (Clock::now() > end) { CHECK(false) << "replicas did not converge"; return {}; }
            std::this_thread::sleep_for(20ms);
        }
    }
};

std::vector<std::vector<std::uint8_t>> seq(std::uint64_t from, std::uint64_t to) {
    std::vector<std::vector<std::uint8_t>> v;
    for (auto i = from; i < to; ++i) v.push_back(payload(i));
    return v;
}

}  // namespace

TEST(elects_a_leader_and_replicates_in_order) {
    Cluster c("basic", 3);
    c.wait_leader();
    for (std::uint64_t i = 0; i < 500; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged() == seq(0, 500));
}

TEST(a_single_node_cluster_works) {
    Cluster c("single", 1);
    c.wait_leader();
    for (std::uint64_t i = 0; i < 100; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged().size() == 100);
}

TEST(followers_refuse_proposals_and_point_to_the_leader) {
    Cluster c("redirect", 3);
    auto l = c.wait_leader();
    CHECK(c.propose(payload(0)).ok());
    c.wait_applied({0, 1, 2}, 1);
    std::size_t f = l == 0 ? 1 : 0;
    auto r = c.nodes[f]->propose(payload(1));
    CHECK(r.kind == ProposeResult::Kind::NotLeader && r.hint && *r.hint == l) << "no redirect";
}

TEST(leader_crash_loses_no_acknowledged_command) {
    Cluster c("failover", 3);
    c.wait_leader();
    std::set<std::uint64_t> acked, maybe;
    auto go = [&](std::uint64_t a, std::uint64_t b) {
        for (auto i = a; i < b; ++i) (c.propose(payload(i)).ok() ? acked : maybe).insert(i);
    };
    go(0, 200);
    auto old = c.leader();
    c.crash(old);
    go(200, 600);
    c.restart(old);
    auto got = c.wait_converged(20s);
    std::set<std::uint64_t> gs;
    std::uint64_t prev = 0;
    bool ordered = true, dup = false;
    for (auto& p : got) {
        auto id = id_of(p);
        if (!gs.insert(id).second) dup = true;
        if (id < prev) ordered = false;
        prev = id;
    }
    CHECK(!dup) << "a command applied twice";
    CHECK(ordered) << "commands reordered";
    for (auto a : acked) CHECK(gs.count(a)) << "acknowledged command " << a << " lost";
    for (auto g : gs) CHECK(acked.count(g) || maybe.count(g)) << g << " was never proposed";
}

TEST(an_isolated_leader_cannot_commit_and_its_uncommitted_entries_vanish) {
    Cluster c("partition", 3, [](ReplicaConfig& cfg) { cfg.propose_timeout_ms = 700; });
    auto old = c.wait_leader();
    for (std::uint64_t i = 0; i < 20; ++i) CHECK(c.propose(payload(i)).ok());
    c.wait_applied({0, 1, 2}, 20);
    c.nodes[old]->filter->isolate(static_cast<NodeId>(old), {0, 1, 2});
    auto r = c.nodes[old]->propose(payload(1000));
    CHECK(r.kind == ProposeResult::Kind::Timeout || r.kind == ProposeResult::Kind::NotLeader || r.kind == ProposeResult::Kind::Superseded)
        << "an isolated leader must not acknowledge";
    // The majority elects a new leader and commits.
    const auto end = Clock::now() + 10s;
    int newl = -1;
    while (newl < 0 && Clock::now() < end) {
        for (std::size_t i = 0; i < 3; ++i) if (i != old && c.nodes[i]->status().role == orderer::consensus::Role::Leader) newl = int(i);
        std::this_thread::sleep_for(20ms);
    }
    CHECK(newl >= 0) << "majority did not elect a leader";
    for (std::uint64_t i = 20; i < 60; ++i) {
        for (;;) {
            auto p = c.nodes[newl]->propose(payload(i));
            if (p.ok()) break;
            CHECK(p.kind == ProposeResult::Kind::Busy || p.kind == ProposeResult::Kind::NotLeader) << "majority side failed";
            std::this_thread::sleep_for(5ms);
        }
    }
    c.nodes[old]->filter->heal();
    CHECK(c.wait_converged(15s) == seq(0, 60)) << "entry 1000 must not survive";
}

TEST(a_lagging_node_catches_up_by_snapshot) {
    Cluster c("snapcatch", 3, [](ReplicaConfig& cfg) { cfg.compact_every = 40; });
    auto l = c.wait_leader();
    std::size_t lag = l == 0 ? 1 : 0;
    for (std::uint64_t i = 0; i < 10; ++i) CHECK(c.propose(payload(i)).ok());
    c.wait_applied({0, 1, 2}, 10);
    c.stop(lag);
    for (std::uint64_t i = 10; i < 400; ++i) CHECK(c.propose(payload(i)).ok());
    const auto end = Clock::now() + 10s;
    while (c.nodes[l]->status().base == 0 && Clock::now() < end) std::this_thread::sleep_for(20ms);
    CHECK(c.nodes[l]->status().base > 0) << "leader never compacted";
    c.restart(lag);
    CHECK(c.wait_converged(20s) == seq(0, 400));
    CHECK(c.nodes[lag]->status().base > 0) << "caught up without a snapshot install";
}

TEST(a_whole_cluster_restart_recovers_from_disk) {
    Cluster c("restart", 3, [](ReplicaConfig& cfg) { cfg.compact_every = 50; cfg.fsync = true; });
    c.wait_leader();
    for (std::uint64_t i = 0; i < 300; ++i) CHECK(c.propose(payload(i)).ok());
    c.wait_converged(10s);
    for (std::size_t i = 0; i < 3; ++i) c.stop(i);
    for (std::size_t i = 0; i < 3; ++i) c.restart(i);
    c.wait_leader(10s);
    for (std::uint64_t i = 300; i < 400; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged(15s) == seq(0, 400));
}

TEST(a_torn_wal_tail_recovers_and_a_corrupt_wal_refuses_to_start) {
    Cluster c("wal", 3, [](ReplicaConfig& cfg) { cfg.fsync = true; });
    auto l = c.wait_leader();
    for (std::uint64_t i = 0; i < 100; ++i) CHECK(c.propose(payload(i)).ok());
    c.wait_converged(10s);
    std::size_t f = l == 0 ? 1 : 0;
    c.crash(f);
    const auto wal = c.dir / ("n" + std::to_string(f)) / "wal" / "raft.wal";
    {   // half a frame of garbage, as a torn write leaves
        std::ofstream o(wal, std::ios::binary | std::ios::app);
        const unsigned char junk[] = {0x40, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3};
        o.write(reinterpret_cast<const char*>(junk), sizeof junk);
    }
    c.restart(f);
    for (std::uint64_t i = 100; i < 150; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged(10s) == seq(0, 150));
    // Now real damage: flip a byte early in the file, with valid frames after it.
    c.stop(f);
    std::string data = read_file(wal);
    data[40] ^= 0xff;
    { std::ofstream o(wal, std::ios::binary | std::ios::trunc); o.write(data.data(), std::streamsize(data.size())); }
    bool refused = false;
    try { auto r = Replica::start(c.config(f), std::make_unique<RecorderSm>()); }
    catch (const StartError&) { refused = true; }
    CHECK(refused) << "a node with mid-log damage must not join";
}

TEST(state_divergence_halts_the_odd_node_out) {
    Cluster c("diverge", 3, [](ReplicaConfig& cfg) { cfg.digest_every = 16; });
    c.stop(2);
    c.shared[2] = std::make_shared<Shared>();
    c.shared[2]->corrupt_at = 5;
    c.start(2);
    c.wait_leader();
    // If the corrupted node happens to lead, it halts itself mid-run and
    // proposals in flight to it have no definite answer; the majority carries on.
    std::size_t acked = 0;
    for (std::uint64_t i = 0; i < 200; ++i) acked += c.propose(payload(i)).ok();
    CHECK(acked >= 150) << "only " << acked << " of 200 acknowledged";
    const auto end = Clock::now() + 10s;
    std::string why;
    while (Clock::now() < end) {
        why = c.nodes[2]->status().halted;
        if (!why.empty()) break;
        std::this_thread::sleep_for(20ms);
    }
    CHECK(why.find("diverged") != std::string::npos) << "divergence went undetected: '" << why << "'";
    CHECK(c.nodes[0]->status().halted.empty() && c.nodes[1]->status().halted.empty());
}

TEST(matching_state_passes_digest_checks) {
    Cluster c("digest-ok", 3, [](ReplicaConfig& cfg) { cfg.digest_every = 16; });
    c.wait_leader();
    for (std::uint64_t i = 0; i < 200; ++i) CHECK(c.propose(payload(i)).ok());
    c.wait_converged(10s);
    const auto end = Clock::now() + 5s;
    while (c.nodes[0]->status().digests_verified == 0 && Clock::now() < end) std::this_thread::sleep_for(20ms);
    CHECK(c.nodes[0]->status().digests_verified > 0) << "no digest was ever verified";
    for (std::size_t i = 0; i < 3; ++i) CHECK(c.nodes[i]->status().halted.empty());
}

TEST(hostile_connections_do_not_disturb_the_cluster) {
    Cluster c("hostile", 3);
    c.wait_leader();
    const Addr target = c.members[0].second;
    std::vector<std::vector<std::uint8_t>> junk = {
        std::vector<std::uint8_t>(64, 0xff),
        {0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0, 1},
        {10, 0, 0, 0, 0, 0, 0, 0, 6, 9, 1, 2, 3, 4, 5, 6, 7, 8},
        {},
    };
    for (auto& j : junk) {
        Fd s = connect_to(target, 500);
        if (s.ok() && !j.empty()) { try { send_all(s.get(), j.data(), j.size()); } catch (...) {} }
    }
    for (std::uint64_t i = 0; i < 100; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged(10s).size() == 100);
}

TEST(five_nodes_survive_two_failures_and_three_cannot_commit_with_two_down) {
    {
        Cluster c("five", 5);
        c.wait_leader();
        for (std::uint64_t i = 0; i < 100; ++i) CHECK(c.propose(payload(i)).ok());
        auto l = c.leader();
        c.crash(l);
        std::size_t other = 0;
        while (other == std::size_t(l)) ++other;
        c.crash(other);
        c.wait_leader(15s);
        for (std::uint64_t i = 100; i < 200; ++i) CHECK(c.propose(payload(i)).ok());
        CHECK(c.wait_converged(15s) == seq(0, 200));
    }
    {
        Cluster c("noquorum", 3, [](ReplicaConfig& cfg) { cfg.propose_timeout_ms = 500; });
        auto l = c.wait_leader();
        CHECK(c.propose(payload(0)).ok());
        for (std::size_t i = 0; i < 3; ++i) if (i != l) c.crash(i);
        auto r = c.nodes[l]->propose(payload(1));
        CHECK(r.kind == ProposeResult::Kind::Timeout) << "no quorum, no ack";
        CHECK(c.shared[l]->payloads() == seq(0, 1));
    }
}

TEST(a_slow_leader_state_machine_does_not_cost_it_leadership) {
    Cluster c("slow-leader", 3, [](ReplicaConfig& cfg) {
        cfg.max_pending = 100000;
        cfg.propose_timeout_ms = 30000;
        cfg.consensus.election_ticks = cfg.id == 0 ? 8 : 40;  // node 0 wins the first election
    });
    c.wait_leader();
    // The slow machine is node 0's; each apply outlasts the followers' election timeout.
    auto l = c.wait_leader();
    CHECK(l == 0) << "node 0 should lead";
    c.shared[0]->apply_delay_ms = 700;
    const Term term0 = c.nodes[0]->status().term;
    std::vector<std::future<ProposeResult>> fs;
    for (std::uint64_t i = 0; i < 3; ++i) fs.push_back(c.nodes[0]->propose_async(payload(i)));
    for (auto& f : fs) CHECK(f.get().ok());
    auto s = c.nodes[0]->status();
    CHECK(s.role == orderer::consensus::Role::Leader && s.term == term0) << "leadership was lost while the state machine was slow";
    c.wait_applied({1, 2}, 3, 5s);
    c.wait_applied({0}, 3, 30s);
}

TEST(a_cluster_with_one_shared_key_works_and_a_wrong_key_cannot_join) {
    const std::vector<std::uint8_t> key = {'r', 'i', 'g', 'h', 't'};
    Cluster c("auth", 3, [key](ReplicaConfig& cfg) { cfg.auth_key = key; });
    auto l = c.wait_leader();
    for (std::uint64_t i = 0; i < 100; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.wait_converged(10s).size() == 100);
    std::size_t f = l == 0 ? 1 : 0;
    c.stop(f);
    auto cfg = c.config(f);
    cfg.auth_key = std::vector<std::uint8_t>{'w', 'r', 'o', 'n', 'g'};
    auto rogue = Replica::start(cfg, std::make_unique<RecorderSm>());
    std::this_thread::sleep_for(1500ms);
    CHECK(rogue->status().commit == 0) << "a node with the wrong key received log entries";
    const Term term = c.nodes[l]->status().term;
    for (std::uint64_t i = 100; i < 140; ++i) CHECK(c.propose(payload(i)).ok());
    CHECK(c.nodes[l]->status().term == term) << "an outsider disturbed the cluster";
}

TEST(the_client_protocol_sessions_and_linearizable_reads) {
    Cluster c("client", 3, nullptr, /*dedup=*/true);
    auto l = c.wait_leader();
    auto addrs = free_addrs(3);
    std::vector<std::unique_ptr<ClientServer>> servers;
    std::vector<std::pair<NodeId, Addr>> pairs;
    for (std::size_t i = 0; i < 3; ++i) {
        servers.push_back(std::make_unique<ClientServer>(addrs[i], *c.nodes[i]));
        pairs.emplace_back(NodeId(i), addrs[i]);
    }
    // Followers first: the client must find the leader by hint.
    std::vector<std::pair<NodeId, Addr>> order;
    for (std::size_t i = 0; i < 3; ++i) if (i != l) order.push_back(pairs[i]);
    order.push_back(pairs[l]);
    Client client(order);
    client.with_session(7);
    auto cmd = [](std::uint64_t id) { return std::vector<CmdSym>{{1, matcher::Command::new_limit(id, matcher::Side::Bid, 10000, 1, matcher::Tif::Gtc)}}; };
    for (std::uint64_t i = 1; i <= 20; ++i) { auto b = cmd(i); CHECK(client.propose_session(b.data(), b.size()).ok()) << i; }
    // The same session batch committed twice (a retry) takes effect once.
    std::vector<std::uint8_t> dup;
    auto b = cmd(999);
    encode_batch(b.data(), b.size(), dup, std::pair<std::uint64_t, std::uint64_t>{8, 1});
    CHECK(c.propose(dup).ok());
    CHECK(c.propose(dup).ok());
    c.wait_converged(10s);
    std::size_t with999 = 0;
    for (auto& p : c.shared[0]->payloads()) for (auto& [s, cm] : decode_batch(p.data(), p.size())) if (cm.order_id == 999) ++with999;
    CHECK(with999 == 1) << "a retried batch took effect " << with999 << " times";
    // Status, hash and barrier over the wire; a follower cannot serve a barrier.
    auto conn = Conn::connect(addrs[l]);
    CHECK(conn.has_value());
    if (conn) {
        for (int i = 0; i < 3; ++i) { auto st = conn->status(); CHECK(st && st->find("role=Leader") != std::string::npos); }
        CHECK(conn->hash().has_value());
        CHECK(conn->barrier().has_value());
    }
    std::size_t f = l == 0 ? 1 : 0;
    auto fc = Conn::connect(addrs[f]);
    CHECK(fc.has_value() && !fc->barrier().has_value());
    // A client without a session cannot use propose_session.
    Client plain(pairs);
    auto b2 = cmd(1);
    CHECK(plain.propose_session(b2.data(), b2.size()).kind == ClientResult::Kind::Protocol);
}

TEST(a_deposed_leader_cannot_serve_a_stale_read) {
    Cluster c("stale", 3, [](ReplicaConfig& cfg) { cfg.propose_timeout_ms = 600; });
    auto old = c.wait_leader();
    CHECK(c.propose(payload(1)).ok());
    c.wait_applied({0, 1, 2}, 1);
    c.nodes[old]->filter->isolate(static_cast<NodeId>(old), {0, 1, 2});
    const auto end = Clock::now() + 10s;
    int newl = -1;
    while (newl < 0 && Clock::now() < end) {
        for (std::size_t i = 0; i < 3; ++i) if (i != old && c.nodes[i]->barrier().ok()) newl = int(i);
        std::this_thread::sleep_for(20ms);
    }
    CHECK(newl >= 0);
    CHECK(c.nodes[newl]->propose(payload(2)).ok());
    CHECK(c.nodes[newl]->barrier().ok());
    CHECK(c.shared[old]->count() == 1);
    CHECK(!c.nodes[old]->barrier().ok()) << "a deposed leader served a barrier";
}

int main() { return check::run_all(); }
