// orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//   orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
// Runs the file through a pipeline (one producer, file order), drains, prints
// every event symbol-tagged, grouped by partition. --snap then writes the
// merged snapshot to PATH and its cut to PATH.meta.
#include <orderer/harness.hpp>

using namespace orderer;
using namespace orderer::harness;

int main(int argc, char** argv) {
    const std::string usage =
        "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]";
    auto valued = COMMON_VALUED;
    valued.push_back("--snap");
    Args a(argc, argv, usage, valued, COMMON_FLAGS);
    if (a.positional.size() != 1) die(usage);
    Corpus c = load_corpus(a.positional[0]);
    Common com = common(a);
    auto snap_path = a.get("--snap");
    auto [listing, snap] = run_corpus(c, com, true, snap_path.has_value());
    print(listing);
    if (snap_path) snap->write(*snap_path);
}
