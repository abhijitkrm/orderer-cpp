// orderer_node — one replica of a replicated cluster, as a process. Same command
// line and client protocol as orderer-rust's orderer-node, so Rust and C++
// nodes can share a cluster.
//
//   orderer_node --id N --members 0=HOST:PORT,1=HOST:PORT,... --client HOST:PORT
//                --dir DIR [--compact-every N] [--digest-every N]
//                [--fsync on|off] [--audit] [--sm pipeline|recorder] [--partitions P]
//
// The state machine is the matching pipeline (default) or the language-neutral
// RecorderSm, wrapped in DedupSm.
// `--audit` appends `(log index, first order id)` for every applied batch to
// DIR/applied.log (truncated at start), as the Rust node does. ORDERER_KEY in
// the environment sets the pre-shared cluster key.
#include <csignal>
#include <fstream>
#include <iostream>

#include "orderer/cluster/client.hpp"
#include "orderer/cluster/pipeline_sm.hpp"

using namespace orderer::cluster;

namespace {

[[noreturn]] void die(const std::string& m) {
    std::cerr << "orderer_node: " << m << "\n";
    std::exit(2);
}

/// Records (index, first order id) of every applied batch, then delegates.
class Audited : public StateMachine {
public:
    Audited(std::unique_ptr<StateMachine> inner, std::unique_ptr<std::ofstream> log) : inner_(std::move(inner)), log_(std::move(log)) {}
    void apply(Index index, const Entry& e) override {
        if (log_) {
            try {
                auto cmds = decode_batch(e.data.data(), e.data.size());
                if (!cmds.empty()) {
                    std::uint8_t rec[16];
                    const std::uint64_t id = cmds.front().second.order_id;
                    std::memcpy(rec, &index, 8);
                    std::memcpy(rec + 8, &id, 8);
                    log_->write(reinterpret_cast<const char*>(rec), 16);
                    log_->flush();  // straight to the OS: survives kill -9 of this process
                }
            } catch (const CodecError&) {}
        }
        inner_->apply(index, e);
    }
    std::vector<std::uint8_t> snapshot(Index i) override { return inner_->snapshot(i); }
    std::optional<std::string> restore(Index i, const std::uint8_t* d, std::size_t n) override { return inner_->restore(i, d, n); }
    std::optional<Digest> digest(Index i) override { return inner_->digest(i); }

private:
    std::unique_ptr<StateMachine> inner_;
    std::unique_ptr<std::ofstream> log_;
};

}  // namespace

int main(int argc, char** argv) {
    ReplicaConfig cfg;
    std::optional<int> id;
    std::optional<Addr> client;
    std::string dir, sm_kind = "pipeline";
    std::uint32_t partitions = 2;
    bool audit = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto v = [&](const char* n) -> std::string { if (++i >= argc) die(std::string(n) + " needs a value"); return argv[i]; };
        if (a == "--id") id = std::stoi(v("--id"));
        else if (a == "--members") {
            std::stringstream ss(v("--members"));
            for (std::string m; std::getline(ss, m, ',');) {
                auto eq = m.find('=');
                if (eq == std::string::npos) die("--members wants ID=HOST:PORT,...");
                cfg.members.emplace_back(static_cast<NodeId>(std::stoi(m.substr(0, eq))), Addr::parse(m.substr(eq + 1)));
            }
        } else if (a == "--client") client = Addr::parse(v("--client"));
        else if (a == "--dir") dir = v("--dir");
        else if (a == "--compact-every") cfg.compact_every = std::stoull(v("--compact-every"));
        else if (a == "--digest-every") cfg.digest_every = std::stoull(v("--digest-every"));
        else if (a == "--fsync") cfg.fsync = v("--fsync") == "on";
        else if (a == "--audit") audit = true;
        else if (a == "--sm") sm_kind = v("--sm");
        else if (a == "--partitions") partitions = static_cast<std::uint32_t>(std::stoul(v("--partitions")));
        else die("unknown argument " + a);
    }
    if (!id || !client || cfg.members.empty() || dir.empty()) die("--id, --members, --client and --dir are required");
    if (sm_kind != "recorder" && sm_kind != "pipeline") die("--sm " + sm_kind + ": want pipeline or recorder");
    cfg.id = static_cast<NodeId>(*id);
    cfg.dir = std::filesystem::path(dir) / "state";
    cfg.max_pending = 1 << 16;
    std::filesystem::create_directories(dir);
    std::optional<std::vector<std::uint8_t>> key;
    if (const char* k = std::getenv("ORDERER_KEY"); k && *k) key = std::vector<std::uint8_t>(k, k + std::strlen(k));
    cfg.auth_key = key;

    std::unique_ptr<std::ofstream> log;
    if (audit) log = std::make_unique<std::ofstream>(std::filesystem::path(dir) / "applied.log", std::ios::binary | std::ios::trunc);
    std::unique_ptr<StateMachine> inner;
    if (sm_kind == "recorder") inner = std::make_unique<RecorderSm>();
    else inner = PipelineSm<orderer::FifoCore>::counting(partitions);
    auto sm = std::make_unique<DedupSm>(std::make_unique<Audited>(std::move(inner), std::move(log)));

    std::unique_ptr<Replica> replica;
    try { replica = Replica::start(cfg, std::move(sm)); }
    catch (const std::exception& e) { die(std::string("start: ") + e.what()); }
    ClientServer server(*client, *replica, key);
    std::cout << "orderer_node " << *id << " ready (client port " << client->str() << ")" << std::endl;
    for (;;) std::this_thread::sleep_for(std::chrono::hours(1));
}
