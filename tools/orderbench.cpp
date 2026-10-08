// orderbench — spec/BENCH.md protocol.
//   orderbench <prefix> --mode core [--tag NAME]
//   orderbench <prefix> --mode pipe --partitions P [--producers N]
//              [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
// orderer-cpp tuning flags (listed in the config column when set):
//   --core fifo|noop  --waits relaxed|low  --batch N  --ingress N --inbox N --outbox N
//   --events on|off   --baseline OPS (core untimed ops/s, for eff)
#include <orderer/harness.hpp>

#include <barrier>
#include <cmath>
#include <sstream>

using namespace orderer;
using namespace orderer::harness;
using Clock = std::chrono::steady_clock;

struct Row {
    std::size_t ops = 0;
    std::uint64_t wall_ns = 0;
    std::vector<std::uint64_t> lat;
    std::optional<double> untimed;
};

static std::uint64_t ns_since(Clock::time_point t) {
    return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t).count());
}

static Row core_mode(const Corpus& setup, const Corpus& run) {
    Row r;
    r.ops = run.cmds.size();
    NullSink sink;
    auto apply_all = [&](auto& target, const std::vector<std::pair<Symbol, Command>>& cmds, std::size_t n) {
        for (std::size_t i = 0; i < n; ++i) {
            if constexpr (std::is_same_v<std::decay_t<decltype(target)>, Engine>)
                target.submit(cmds[i].first, cmds[i].second, sink);
            else
                target.apply(cmds[i].second, sink);
        }
    };
    auto pass = [&](auto make) {
        {  // warmup: setup + 10% of run
            auto t = make();
            apply_all(t, setup.cmds, setup.cmds.size());
            apply_all(t, run.cmds, run.cmds.size() / 10);
        }
        {  // timed per op (matcher protocol)
            auto t = make();
            apply_all(t, setup.cmds, setup.cmds.size());
            r.lat.reserve(run.cmds.size());
            auto wall = Clock::now();
            for (auto& [s, c] : run.cmds) {
                auto t0 = Clock::now();
                if constexpr (std::is_same_v<decltype(t), Engine>) t.submit(s, c, sink);
                else t.apply(c, sink);
                r.lat.push_back(ns_since(t0));
            }
            r.wall_ns = ns_since(wall);
        }
        {  // untimed: the scaling gate's denominator (spec/BENCH.md 1.1)
            auto t = make();
            apply_all(t, setup.cmds, setup.cmds.size());
            auto wall = Clock::now();
            apply_all(t, run.cmds, run.cmds.size());
            r.untimed = double(run.cmds.size()) / (double(ns_since(wall)) / 1e9);
        }
    };
    if (setup.engine) pass([&] { return Engine(setup.book); });
    else pass([&] { return OrderBook(setup.book); });
    if (sink.acc == 42) std::cerr << "";
    return r;
}

struct PipeOpts {
    std::uint32_t partitions = 1;
    std::size_t producers = 1, batch = 64;
    std::optional<JournalConfig> journal;
    Waits waits = Waits::low_latency();
    std::size_t ingress = 1 << 14, inbox = 1 << 12, outbox = 1 << 13;
};

template <class C>
static std::unique_ptr<Pipeline<C>> build(const BookConfig& book, const PipeOpts& o, std::optional<EgressFactory> m) {
    auto b = Pipeline<C>::builder();
    b.book_config(book).partitions(o.partitions).waits(o.waits).ring_sizes(o.ingress, o.inbox, o.outbox);
    if (o.journal) b.journal(*o.journal);
    if (m) b.egress(*m);
    return b.build();
}

template <class C>
static Row pipe_mode(const Corpus& setup, const Corpus& run, const PipeOpts& o) {
    {  // warmup on a throwaway pipeline
        auto p = build<C>(setup.book, o, std::nullopt);
        p->publish_batch(setup.cmds);
        std::vector<std::pair<Symbol, Command>> w(run.cmds.begin(), run.cmds.begin() + run.cmds.size() / 10);
        p->publish_batch(w);
        p->drain();
        p->shutdown();
    }
    auto [mf, results] = metrics(run.cmds.size() + 1024);
    auto p = build<C>(setup.book, o, mf);
    p->publish_batch(setup.cmds);
    p->drain();
    std::vector<std::vector<std::pair<Symbol, Command>>> streams(o.producers);
    for (auto& sc : run.cmds) streams[sc.first % o.producers].push_back(sc);
    p->set_timestamps(true);
    std::barrier start(std::ptrdiff_t(o.producers + 1));
    std::vector<std::thread> ts;
    for (auto& s : streams) {
        ts.emplace_back([&, h = p->handle()]() mutable {
            start.arrive_and_wait();
            for (std::size_t i = 0; i < s.size(); i += o.batch)
                h.publish_batch(s.data() + i, std::min(o.batch, s.size() - i));
        });
    }
    start.arrive_and_wait();
    auto wall = Clock::now();
    for (auto& t : ts) t.join();
    p->drain();
    Row r;
    r.wall_ns = ns_since(wall);
    r.ops = run.cmds.size();
    p->set_timestamps(false);
    p->shutdown();
    for (auto& m : results->results()) r.lat.insert(r.lat.end(), m.latencies.begin(), m.latencies.end());
    return r;
}

static std::uint64_t pct(const std::vector<std::uint64_t>& v, double p) {
    if (v.empty()) return 0;
    std::size_t i = std::size_t(std::ceil(double(v.size() - 1) * p));
    return v[std::min(i, v.size() - 1)];
}

static std::string cpu() {
    std::string out;
    if (FILE* f = popen("sysctl -n machdep.cpu.brand_string 2>/dev/null", "r")) {
        char buf[256];
        while (std::fgets(buf, sizeof buf, f)) out += buf;
        pclose(f);
    }
    while (!out.empty() && (out.back() == '\n' || out.back() == ' ')) out.pop_back();
    return out.empty() ? "unknown cpu" : out;
}

int main(int argc, char** argv) {
    const std::string usage =
        "orderbench <prefix> --mode core|pipe [--partitions P] [--producers N] [--journal binary|jsonl|off] "
        "[--journal-dir DIR] [--fsync N] [--tag NAME] [--core fifo|noop] [--waits relaxed|low] [--batch N] "
        "[--ingress N] [--inbox N] [--outbox N] [--events on|off] [--baseline OPS]";
    Args a(argc, argv, usage,
           {"--mode", "--partitions", "--producers", "--journal", "--journal-dir", "--fsync", "--tag", "--core",
            "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events", "--baseline"},
           {});
    if (a.positional.size() != 1) die(usage);
    std::string prefix = a.positional[0];
    std::string tag = a.get("--tag").value_or(prefix.substr(prefix.rfind('/') + 1));
    Corpus setup = load_corpus(prefix + ".setup.cmd.jsonl");
    Corpus run = load_corpus(prefix + ".run.cmd.jsonl");
    std::string mode = a.get("--mode").value_or("core");
    std::vector<std::string> config;
    Row row;
    std::string P = "-", prod = "-";
    if (mode == "core") {
        row = core_mode(setup, run);
    } else if (mode == "pipe") {
        PipeOpts o;
        o.partitions = a.num<std::uint32_t>("--partitions", 1);
        o.producers = std::max<std::size_t>(a.num<std::size_t>("--producers", 1), 1);
        o.batch = std::max<std::size_t>(a.num<std::size_t>("--batch", 64), 1);
        o.ingress = a.num<std::size_t>("--ingress", o.ingress);
        o.inbox = a.num<std::size_t>("--inbox", o.inbox);
        o.outbox = a.num<std::size_t>("--outbox", o.outbox);
        std::uint64_t fsync = a.num<std::uint64_t>("--fsync", 1024);
        std::string jm = a.get("--journal").value_or("binary");
        auto tmp = fs::temp_directory_path() / ("orderbench-cpp-" + std::to_string(::getpid()));
        if (jm == "binary" || jm == "jsonl") {
            JournalConfig j;
            j.dir = a.get("--journal-dir") ? fs::path(*a.get("--journal-dir")) : tmp;
            j.format = jm == "binary" ? JournalFormat::Binary : JournalFormat::Jsonl;
            j.fsync = fsync ? FsyncPolicy::every_n(fsync) : FsyncPolicy::never();
            j.events = a.get("--events").value_or("off") == "on";
            o.journal = j;
        } else if (jm != "off") {
            die("--journal: unknown mode " + jm);
        }
        std::string w = a.get("--waits").value_or("low");
        if (w == "relaxed") o.waits = Waits::relaxed();
        else if (w != "low") die("--waits: unknown " + w);
        config.push_back("journal=" + jm + " fsync=" + std::to_string(fsync));
        for (std::string k : {"--core", "--waits", "--batch", "--ingress", "--inbox", "--outbox", "--events"})
            if (auto v = a.get(k)) config.push_back(k.substr(2) + "=" + *v);
        std::string core = a.get("--core").value_or("fifo");
        try {
            if (core == "fifo") row = pipe_mode<FifoCore>(setup, run, o);
            else if (core == "noop") row = pipe_mode<NoopCore>(setup, run, o);
            else die("--core: unknown " + core);
        } catch (const std::exception& e) {
            fail(e.what());
        }
        std::error_code ec;
        fs::remove_all(tmp, ec);
        P = std::to_string(o.partitions);
        prod = std::to_string(o.producers);
    } else {
        die("--mode: unknown " + mode);
    }
    if (row.untimed) config.push_back("untimed=" + std::to_string(std::uint64_t(*row.untimed)));
    std::sort(row.lat.begin(), row.lat.end());
    double ops_s = double(row.ops) / (double(row.wall_ns) / 1e9);
    unsigned __int128 sum = 0;
    for (auto v : row.lat) sum += v;
    std::uint64_t mean = row.lat.empty() ? 0 : std::uint64_t(sum / row.lat.size());
    std::string eff;
    if (mode == "pipe")
        if (auto b = a.get("--baseline")) {
            char buf[32];
            std::snprintf(buf, sizeof buf, "%.2f", ops_s / (std::stod(P) * std::stod(*b)));
            eff = buf;
        }
    std::ostringstream cfg;
    for (std::size_t i = 0; i < config.size(); ++i) cfg << (i ? " " : "") << config[i];
    std::printf("| %s | %s | %s | %s | %zu | %.0f | %s | %llu | %llu | %llu | %llu | %llu | %llu | %s |\n",
                tag.c_str(), mode.c_str(), P.c_str(), prod.c_str(), row.ops, ops_s, eff.c_str(),
                (unsigned long long)mean, (unsigned long long)pct(row.lat, 0.5), (unsigned long long)pct(row.lat, 0.9),
                (unsigned long long)pct(row.lat, 0.99), (unsigned long long)pct(row.lat, 0.999),
                (unsigned long long)(row.lat.empty() ? 0 : row.lat.back()), cfg.str().c_str());
    std::fprintf(stderr, "env: %s / orderer-cpp 0.1.0\n", cpu().c_str());
}
