// common.hpp — test helpers: vectors, a seeded adversarial generator,
// single-Engine references, per-symbol views, pipeline runs.
#pragma once

#include <orderer/harness.hpp>

#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "check.hpp"

namespace t {

using namespace orderer;
using Cmds = std::vector<std::pair<Symbol, Command>>;

inline std::string slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

inline std::vector<std::string> lines(const std::string& s) {
    std::vector<std::string> out;
    std::size_t pos = 0;
    while (pos < s.size()) {
        auto e = s.find('\n', pos);
        if (e == std::string::npos) e = s.size();
        out.push_back(s.substr(pos, e - pos));
        pos = e + 1;
    }
    return out;
}

/// xorshift64* (the tools' generator family).
struct Rng {
    std::uint64_t x;
    std::uint64_t next() { x ^= x >> 12; x ^= x << 25; x ^= x >> 27; return x * 0x2545F4914F6CDD1Dull; }
    std::uint64_t below(std::uint64_t n) { return next() % n; }
};

inline BookConfig fuzz_cfg() { return BookConfig{1, 200, 4096, IndexKind::Ladder}; }

/// Adversarial engine stream: crossing prices, small id space, every TIF,
/// markets, ~5% malformed.
inline Cmds fuzz_corpus(std::uint64_t seed, std::size_t n, std::uint32_t symbols) {
    Rng r{(seed * 0x9E3779B97F4A7C15ull) | 1};
    Cmds out;
    for (std::size_t i = 0; i < n; ++i) {
        Symbol sym = Symbol(r.below(symbols));
        std::uint64_t id = r.below(256);
        std::int64_t price = r.below(20) == 0 ? std::array<std::int64_t, 3>{0, 201, -5}[r.below(3)]
                                              : 90 + std::int64_t(r.below(21));
        std::uint64_t qty = r.below(25) == 0 ? 0 : r.below(50) + 1;
        Command c;
        switch (r.below(10)) {
            case 0: case 1: case 2: case 3: case 4: {
                Side side = r.below(2) ? Side::Ask : Side::Bid;
                if (r.below(8) == 0) c = Command::new_market(id, side, qty);
                else {
                    Tif tifs[] = {Tif::Gtc, Tif::Gtc, Tif::Ioc, Tif::Fok, Tif::PostOnly};
                    c = Command::new_limit(id, side, price, qty, tifs[r.below(5)]);
                }
                break;
            }
            case 5: case 6: case 7: c = Command::cancel(id); break;
            default: c = Command::replace(id, price, qty);
        }
        out.emplace_back(sym, c);
    }
    return out;
}

inline std::vector<std::string> reference_lines(BookConfig cfg, const Cmds& cmds) {
    Engine eng(cfg);
    std::vector<std::string> out;
    for (auto& [s, c] : cmds)
        eng.submit_tagged(s, c, [&](Symbol sym, std::uint64_t seq, const Event& ev) {
            std::string l;
            write_canonical_sym(seq, sym, ev, l);
            out.push_back(std::move(l));
        });
    return out;
}

inline std::string reference_snapshot(BookConfig cfg, const Cmds& cmds, std::size_t n) {
    Engine eng(cfg);
    NullSink sink;
    for (std::size_t i = 0; i < n; ++i) eng.submit(cmds[i].first, cmds[i].second, sink);
    std::string out;
    snapshot::write_engine(eng, out);
    return out;
}

inline std::map<std::uint64_t, std::vector<std::string>> by_symbol(const std::vector<std::string>& ls) {
    std::map<std::uint64_t, std::vector<std::string>> m;
    for (auto& l : ls) m[flat::get_u64(l, "symbol").value_or(0)].push_back(l);
    return m;
}

inline bool dense(const std::vector<std::string>& ls) {
    std::map<std::uint64_t, std::uint64_t> next;
    for (auto& l : ls) {
        auto sym = flat::get_u64(l, "symbol").value_or(0);
        if (*flat::get_u64(l, "seq") != ++next[sym]) return false;
    }
    return true;
}

/// Per-partition canonical lines from a pipeline run.
template <MatchingCore C = FifoCore>
std::vector<std::vector<std::string>> run_pipeline(BookConfig cfg, const Cmds& cmds, std::uint32_t P, bool tagged) {
    auto [f, h] = collect(tagged);
    auto p = Pipeline<C>::builder().book_config(cfg).partitions(P).ring_sizes(1 << 10, 1 << 8, 1 << 8).egress(f).build();
    p->publish_batch(cmds);
    p->drain();
    p->shutdown();
    std::vector<std::vector<std::string>> out;
    for (auto& b : h->take()) out.push_back(lines(b));
    return out;
}

inline std::vector<std::string> concat(const std::vector<std::vector<std::string>>& v) {
    std::vector<std::string> out;
    for (auto& p : v) out.insert(out.end(), p.begin(), p.end());
    return out;
}

inline fs::path scratch(const fs::path& root, const std::string& name) {
    auto d = root / name;
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

}  // namespace t
