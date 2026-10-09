// ordersnap — spec/HARNESS.md §4.4 (mirrors matchersnap).
//   ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
// Runs the file, drains, prints the merged matcher-snap/1 snapshot.
#include <orderer/harness.hpp>

using namespace orderer;
using namespace orderer::harness;

int main(int argc, char** argv) {
    const std::string usage =
        "ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
    Args a(argc, argv, usage, COMMON_VALUED, COMMON_FLAGS);
    if (a.positional.size() != 1) die(usage);
    Corpus c = load_corpus(a.positional[0]);
    Common com = common(a);
    RunOpts opts;
    opts.snapshot = true;
    print(run_corpus(c, com, true, opts).second->body);
}
