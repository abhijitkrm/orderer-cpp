// A1 — every vendored matcher vector (and orderer's regress vectors), every
// index mode, through the live pipeline: P=1 byte-identical to .evt; P=4
// per-symbol identical (engine) / unchanged (single-book), routing honored.
//   golden_test <vectors-dir>
#include "common.hpp"

namespace fs = std::filesystem;
static fs::path VEC;

static void run_vector(const fs::path& cmd, const fs::path& evt, int& runs) {
    using namespace t;
    std::string text = slurp(cmd);
    harness::Corpus c;
    if (auto e = harness::parse_corpus(text, cmd.string(), c)) { CHECK(false) << *e; return; }
    auto expected = lines(slurp(evt));
    expected.erase(expected.begin());
    const std::string header = text.substr(0, text.find('\n'));
    auto ix = flat::get_str(header, "index");
    std::vector<IndexKind> modes{IndexKind::Ladder};
    if (ix && *ix == "both") modes = {IndexKind::Ladder, IndexKind::Tree};
    if (ix && *ix == "tree") modes = {IndexKind::Tree};
    for (auto kind : modes) {
        BookConfig cfg = c.book;
        cfg.index = kind;
        CHECK(concat(run_pipeline(cfg, c.cmds, 1, c.engine)) == expected) << cmd << " P=1";
        auto parts = run_pipeline(cfg, c.cmds, 4, c.engine);
        if (c.engine) {
            CHECK(by_symbol(concat(parts)) == by_symbol(expected)) << cmd << " P=4";
            for (std::uint32_t p = 0; p < parts.size(); ++p)
                for (auto& l : parts[p])
                    CHECK(hash_partition(Symbol(*flat::get_u64(l, "symbol")), 4) == p) << "routing " << l;
        } else {
            CHECK(concat(parts) == expected) << cmd << " P=4 single-book";
        }
        ++runs;
    }
}

TEST(every_vector_through_pipeline) {
    int runs = 0;
    std::string mf = t::slurp(VEC / "matcher/manifest.json");
    for (std::size_t pos = 0; (pos = mf.find("\"file\"", pos)) != std::string::npos;) {
        auto a = mf.find('"', mf.find(':', pos) + 1) + 1;
        auto b = mf.find('"', a);
        std::string name = mf.substr(a, b - a);
        pos = b;
        run_vector(VEC / "matcher" / (name + ".cmd.jsonl"), VEC / "matcher" / (name + ".evt.jsonl"), runs);
    }
    for (auto& e : fs::directory_iterator(VEC / "regress")) {
        auto p = e.path().string();
        if (p.size() > 10 && p.compare(p.size() - 10, 10, ".cmd.jsonl") == 0)
            run_vector(e.path(), p.substr(0, p.size() - 10) + ".evt.jsonl", runs);
    }
    CHECK(runs >= 80) << "only " << runs << " vector runs";
    std::cout << "     " << runs << " vector runs\n";
}

TEST(routing_vectors) {
    using namespace t;
    int n = 0;
    for (auto& l : lines(slurp(VEC / "routing/hash.jsonl"))) {
        if (l.find("\"format\"") != std::string::npos) continue;
        auto s = Symbol(*flat::get_u64(l, "symbol"));
        auto P = std::uint32_t(*flat::get_u64(l, "partitions"));
        auto want = std::uint32_t(*flat::get_u64(l, "partition"));
        PartitionMap m;
        CHECK(!PartitionMap::make(P, {}, m));
        CHECK(hash_partition(s, P) == want && m.partition(s) == want) << l;
        ++n;
    }
    CHECK(n > 500);
    PartitionMap m;
    CHECK(!PartitionMap::parse_table(slurp(VEC / "routing/table.map.jsonl"), 4, m));
    for (auto& l : lines(slurp(VEC / "routing/table.expect.jsonl"))) {
        if (l.find("\"format\"") != std::string::npos) continue;
        CHECK(m.partition(Symbol(*flat::get_u64(l, "symbol"))) == *flat::get_u64(l, "partition")) << l;
    }
    PartitionMap bad;
    CHECK(PartitionMap::parse_table(slurp(VEC / "routing/table.map.jsonl"), 3, bad)) << "P mismatch must fail";
}

int main(int argc, char** argv) {
    VEC = argc > 1 ? argv[1] : "vectors";
    return check::run_all();
}
