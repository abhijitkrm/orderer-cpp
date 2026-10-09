// orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//   orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//            [--checkpoint-every K] [--durable]
// Runs the file through a pipeline (one producer, file order), drains, prints
// every event symbol-tagged, grouped by partition. --snap then writes the
// merged snapshot to PATH and its cut to PATH.meta.
#include <orderer/harness.hpp>

using namespace orderer;
using namespace orderer::harness;

int main(int argc, char** argv) {
    const std::string usage =
        "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH] "
        "[--checkpoint-every K] [--durable]";
    auto valued = COMMON_VALUED;
    valued.push_back("--snap");
    valued.push_back("--checkpoint-every");
    auto flags = COMMON_FLAGS;
    flags.push_back("--durable");
    Args a(argc, argv, usage, valued, flags);
    if (a.positional.size() != 1) die(usage);
    Corpus c = load_corpus(a.positional[0]);
    Common com = common(a);
    auto snap_path = a.get("--snap");
    RunOpts opts;
    opts.snapshot = snap_path.has_value();
    if (a.get("--checkpoint-every")) opts.checkpoint_every = a.num<std::size_t>("--checkpoint-every", 0);
    opts.durable = a.flag("--durable");
    auto [listing, snap] = run_corpus(c, com, true, opts);
    print(listing);
    if (snap_path) snap->write(*snap_path);
}
