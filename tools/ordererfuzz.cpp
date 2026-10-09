// ordererfuzz — spec/HARNESS.md §4.2 (mirrors matcherfuzz).
//   ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
// Like orderrun, but symbol-tagged only for engine files. The CHECKED build
// (scripts/build-harness.sh) runs it under AddressSanitizer + UBSan.
#include <orderer/harness.hpp>

using namespace orderer;
using namespace orderer::harness;

int main(int argc, char** argv) {
    const std::string usage =
        "ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";
    Args a(argc, argv, usage, COMMON_VALUED, COMMON_FLAGS);
    if (a.positional.size() != 1) die(usage);
    Corpus c = load_corpus(a.positional[0]);
    Common com = common(a);
    print(run_corpus(c, com, c.engine, RunOpts{}).first);
}
