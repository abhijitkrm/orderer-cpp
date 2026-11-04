// clusterbench — throughput of the replicated path, in one process over
// loopback TCP (the C++ counterpart of orderer-rust's clusterbench, same
// flags and same output).
//
//   clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
//                [--batch CMDS] [--producers K] [--window W]
//                [--cmds TOTAL] [--fsync on|off] [--dir PATH]
//
// G independent replicated groups of N nodes (symbols sharded between them).
// For each group, K producers keep W batches of CMDS commands in flight
// against its leader. Everything shares one machine: use the numbers as a
// floor for dedicated hardware per replica.
#include <iostream>

#include "orderer/cluster/pipeline_sm.hpp"
#include "orderer/cluster/replica.hpp"

using namespace orderer::cluster;
namespace fs = std::filesystem;

namespace {

[[noreturn]] void die(const std::string& m) { std::cerr << m << "\n"; std::exit(2); }

class NoopSm : public StateMachine {
public:
    void apply(Index, const Entry&) override {}
    std::vector<std::uint8_t> snapshot(Index) override { return {}; }
    std::optional<std::string> restore(Index, const std::uint8_t*, std::size_t) override { return std::nullopt; }
};

// splitmix64-based group assignment, same as orderer-rust's ShardMap.
std::uint32_t group_of(std::uint32_t sym, std::uint32_t groups) {
    std::uint64_t z = std::uint64_t(sym) + 0x9E3779B97F4A7C15ull;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    z ^= z >> 31;
    return static_cast<std::uint32_t>(z % groups);
}

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

}  // namespace

int main(int argc, char** argv) {
    std::size_t nodes = 3, groups = 1, batch = 1000, producers = 2, window = 8;
    std::uint32_t partitions = 2;
    std::uint64_t total = 20'000'000;
    bool fsync = true;
    std::string sm = "noop", dir;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto v = [&]() -> std::string { if (++i >= argc) die(a + " needs a value"); return argv[i]; };
        if (a == "--nodes") nodes = std::stoul(v());
        else if (a == "--groups") groups = std::stoul(v());
        else if (a == "--sm") sm = v();
        else if (a == "--partitions") partitions = static_cast<std::uint32_t>(std::stoul(v()));
        else if (a == "--batch") batch = std::stoul(v());
        else if (a == "--producers") producers = std::stoul(v());
        else if (a == "--window") window = std::stoul(v());
        else if (a == "--cmds") total = std::stoull(v());
        else if (a == "--fsync") fsync = v() == "on";
        else if (a == "--dir") dir = v();
        else die("unknown argument " + a);
    }
    const fs::path root = dir.empty() ? fs::temp_directory_path() / ("clusterbench-cpp-" + std::to_string(::getpid())) : fs::path(dir);
    fs::remove_all(root);
    struct Cleanup { fs::path p; ~Cleanup() { std::error_code ec; fs::remove_all(p, ec); } } cleanup{root};

    std::vector<std::vector<std::unique_ptr<Replica>>> all(groups);
    for (std::size_t g = 0; g < groups; ++g) {
        auto addrs = free_addrs(nodes);
        std::vector<std::pair<NodeId, Addr>> members;
        for (std::size_t i = 0; i < nodes; ++i) members.emplace_back(NodeId(i), addrs[i]);
        for (std::size_t i = 0; i < nodes; ++i) {
            ReplicaConfig cfg;
            cfg.id = NodeId(i);
            cfg.members = members;
            cfg.cluster_id = 1000 + g;
            cfg.dir = root / ("g" + std::to_string(g) + "n" + std::to_string(i));
            cfg.consensus = orderer::consensus::Config{30, 3, 512, true};
            cfg.fsync = fsync;
            cfg.max_pending = 1u << 20;
            cfg.propose_timeout_ms = 60000;
            std::unique_ptr<StateMachine> machine;
            if (sm == "noop") machine = std::make_unique<NoopSm>();
            else if (sm == "pipeline") machine = PipelineSm<orderer::FifoCore>::counting(partitions);
            else die("--sm " + sm);
            all[g].push_back(Replica::start(cfg, std::move(machine)));
        }
    }
    std::vector<std::size_t> leaders(groups);
    for (std::size_t g = 0; g < groups; ++g) {
        const auto end = Clock::now() + std::chrono::seconds(20);
        for (;;) {
            bool found = false;
            for (std::size_t i = 0; i < nodes; ++i)
                if (all[g][i]->status().role == orderer::consensus::Role::Leader) { leaders[g] = i; found = true; }
            if (found) break;
            if (Clock::now() > end) die("no leader");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    const std::uint64_t per_producer = total / (groups * producers);
    std::atomic<std::uint64_t> acked{0}, lat_ns{0}, batches{0};
    const auto t0 = Clock::now();
    std::vector<std::thread> threads;
    for (std::size_t g = 0; g < groups; ++g) {
        Replica* leader = all[g][leaders[g]].get();
        std::vector<std::uint32_t> syms;
        for (std::uint32_t s = 0; s < 4096 && syms.size() < 64; ++s) if (group_of(s, std::uint32_t(groups)) == g) syms.push_back(s);
        for (std::size_t p = 0; p < producers; ++p) {
            threads.emplace_back([=, &acked, &lat_ns, &batches] {
                std::uint64_t next_id = (std::uint64_t(g * 64 + p + 1)) << 36;
                std::uint64_t x = 0x9E3779B97F4A7C15ull ^ (g * 64 + p);
                std::uint64_t sent = 0;
                struct Flight { Clock::time_point t; std::future<ProposeResult> f; };
                std::deque<Flight> inflight;
                std::vector<CmdSym> cmds;
                std::vector<std::uint8_t> payload;
                while (sent < per_producer || !inflight.empty()) {
                    while (sent < per_producer && inflight.size() < window) {
                        cmds.clear();
                        for (std::size_t k = 0; k < batch; ++k) {
                            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                            const auto sym = syms[x % syms.size()];
                            ++next_id;
                            const auto side = (x & 1) == 0 ? matcher::Side::Bid : matcher::Side::Ask;
                            const std::int64_t price = 10000 + std::int64_t((x >> 8) % 8) - 4;
                            cmds.emplace_back(sym, matcher::Command::new_limit(next_id, side, price, 1 + (x >> 20) % 10, matcher::Tif::Gtc));
                        }
                        payload.clear();
                        encode_batch(cmds.data(), cmds.size(), payload);
                        inflight.push_back({Clock::now(), leader->propose_async(payload)});
                        sent += batch;
                    }
                    if (inflight.empty()) break;
                    auto fl = std::move(inflight.front());
                    inflight.pop_front();
                    auto r = fl.f.get();
                    if (r.ok()) {
                        acked += batch;
                        lat_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - fl.t).count();
                        ++batches;
                    } else if (r.kind == ProposeResult::Kind::Busy) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    } else die("proposal failed");
                }
            });
        }
    }
    for (auto& t : threads) t.join();
    const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
    const auto done = acked.load();
    const auto nb = std::max<std::uint64_t>(batches.load(), 1);
    const auto end = Clock::now() + std::chrono::seconds(180);
    for (std::size_t g = 0; g < groups; ++g) {
        const Index want = all[g][leaders[g]]->status().commit;
        for (;;) {
            bool caught = true;
            for (auto& r : all[g]) caught = caught && r->status().applied >= want;
            if (caught) break;
            if (Clock::now() > end) die("followers did not catch up");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    const double applied_secs = std::chrono::duration<double>(Clock::now() - t0).count();
    std::printf(
        "groups=%zu nodes=%zu sm=%s batch=%zu producers=%zu/group window=%zu fsync=%s: %.2fM cmds/s acked (%.2fM per group), "
        "%.2fM applied everywhere, %.0f MB/s log payload, mean batch latency %.2f ms, %llu cmds in %.2fs\n",
        groups, nodes, sm.c_str(), batch, producers, window, fsync ? "on" : "off", done / secs / 1e6, done / secs / 1e6 / double(groups),
        done / applied_secs / 1e6, double(done) * double(CMD_BYTES) / 1e6 / secs, double(lat_ns.load()) / double(nb) / 1e6,
        static_cast<unsigned long long>(done), secs);
    return 0;
}
