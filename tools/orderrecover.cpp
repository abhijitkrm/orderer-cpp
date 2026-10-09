// orderrecover — spec/HARNESS.md §4.3 (mirrors matcherrecover).
//   orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]
//   orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]
// Tail form: restore, submit every tail line without "format" through a
// pipeline, print the replayed events. Journal form: recover from journals
// (after the optional snapshot's cut; by default the directory's newest
// checkpoint). Malformed/corrupt input exits 2; --repair first truncates
// torn tails (spec/JOURNAL.md §5.1).
#include <orderer/harness.hpp>

using namespace orderer;
using namespace orderer::harness;

static void tail_form(const std::string& snap_path, const std::string& tail_path, const PartitionMap& map) {
    Snapshot snap;
    std::pair<BookConfig, std::vector<FifoCore>> restored;
    try {
        snap = read_snapshot(snap_path);
        restored = restore<FifoCore>(BookConfig{}, map, &snap);
    } catch (const std::exception& e) {
        die(e.what());
    }
    std::string text;
    if (auto e = read_text(tail_path, text)) die(*e);
    std::vector<std::pair<Symbol, Command>> cmds;
    std::size_t pos = 0, n = 0;
    while (pos < text.size()) {
        auto e = text.find('\n', pos);
        if (e == std::string::npos) e = text.size();
        std::string_view line(text.data() + pos, e - pos);
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        pos = e + 1;
        ++n;
        if (line.empty() || line.find("\"format\"") != std::string_view::npos) continue;
        auto cmd = flat::parse_command(line);
        auto sym = flat::get_u64(line, "symbol").value_or(0);
        if (!cmd || sym > UINT32_MAX)
            die(tail_path + ":" + std::to_string(n) + ": malformed journal line: " + std::string(line));
        cmds.emplace_back(Symbol(sym), *cmd);
    }
    auto [f, events] = collect(true);
    auto b = Pipeline<FifoCore>::builder();
    b.book_config(restored.first).partition_map(map).egress(f).initial(
        Initial<FifoCore>{std::move(restored.second), snap.iseq + 1});
    try {
        auto p = b.build();
        if (p->publish_batch(cmds) != Status::Ok) fail("pipeline closed");
        p->drain();
        p->shutdown();
    } catch (const std::exception& e) {
        fail(e.what());
    }
    print(events->listing());
}

static void journal_form(const std::string& dir, const std::optional<std::string>& snap_path, JournalFormat fmt,
                         bool repair, const PartitionMap& map) {
    std::vector<std::string> parts(map.partitions());
    try {
        if (repair)
            for (auto& [path, bytes] : repair_dir(dir, fmt))
                std::fprintf(stderr, "repaired %s %llu\n", path.c_str(), (unsigned long long)bytes);
        std::optional<Snapshot> snap;
        if (snap_path) snap = read_snapshot(*snap_path);
        else if (auto cps = list_checkpoints(dir); !cps.empty()) snap = read_snapshot(cps.back().second);
        recover<FifoCore>(BookConfig{}, map, snap ? &*snap : nullptr,
                          std::make_optional(std::make_pair(fs::path(dir), fmt)),
                          [&](std::uint32_t p, Symbol s, std::uint64_t seq, const Event& ev) {
                              write_canonical_sym(seq, s, ev, parts[p]);
                              parts[p] += '\n';
                          });
    } catch (const std::exception& e) {
        die(e.what());
    }
    std::string out;
    for (auto& p : parts) out += p;
    print(out);
}

int main(int argc, char** argv) {
    const std::string usage =
        "orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]\n"
        "       orderrecover --journal-dir DIR [--snap PATH] [--binary] [--repair] [--partitions P] [--partition-map F]";
    Args a(argc, argv, usage, {"--partitions", "--partition-map", "--journal-dir", "--snap"}, {"--binary", "--repair"});
    PartitionMap map = partition_map(a);
    if (auto dir = a.get("--journal-dir"); dir && a.positional.empty()) {
        journal_form(*dir, a.get("--snap"), a.flag("--binary") ? JournalFormat::Binary : JournalFormat::Jsonl,
                     a.flag("--repair"), map);
    } else if (!dir && a.positional.size() == 2 && !a.get("--snap") && !a.flag("--binary") && !a.flag("--repair")) {
        tail_form(a.positional[0], a.positional[1], map);
    } else {
        die(usage);
    }
}
