// harness.hpp — shared plumbing for the spec/HARNESS.md tools (tools/*.cpp).
#pragma once

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "pipeline.hpp"
#include "recover.hpp"

namespace orderer::harness {

/// spec/HARNESS.md §5: usage / input / config / corruption errors exit 2.
[[noreturn]] inline void die(const std::string& msg) {
    std::cerr << msg << "\n";
    std::exit(2);
}
/// Internal failures exit 1.
[[noreturn]] inline void fail(const std::string& msg) {
    std::cerr << msg << "\n";
    std::exit(1);
}

struct Corpus {
    BookConfig book;
    bool engine = false;
    std::vector<std::pair<Symbol, Command>> cmds;
};

inline std::optional<std::string> read_text(const std::string& path, std::string& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return path + ": cannot read";
    out.assign(std::istreambuf_iterator<char>(in), {});
    return std::nullopt;
}

/// A command file (spec/HARNESS.md §1), strictly.
inline std::optional<std::string> parse_corpus(const std::string& text, const std::string& path, Corpus& c) {
    std::size_t pos = 0, n = 0;
    std::string_view t(text);
    auto next = [&](std::string_view& line) {
        if (pos >= t.size()) return false;
        auto e = t.find('\n', pos);
        if (e == std::string_view::npos) e = t.size();
        line = t.substr(pos, e - pos);
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        pos = e + 1;
        ++n;
        return true;
    };
    std::string_view hdr;
    if (!next(hdr)) return path + ": empty file";
    c.book = flat::parse_header(hdr);
    c.engine = flat::get_str(hdr, "engine") == std::optional<std::string_view>("true");
    std::string_view line;
    while (next(line)) {
        if (flat::trim(line).empty()) continue;
        auto cmd = flat::parse_command(line);
        if (!cmd) return path + ":" + std::to_string(n) + ": malformed command: " + std::string(line);
        Symbol sym = 0;
        if (c.engine) {
            auto s = flat::get_u64(line, "symbol");
            if (!s || *s > UINT32_MAX) return path + ":" + std::to_string(n) + ": missing symbol: " + std::string(line);
            sym = Symbol(*s);
        }
        c.cmds.emplace_back(sym, *cmd);
    }
    return std::nullopt;
}

inline Corpus load_corpus(const std::string& path) {
    std::string text;
    if (auto e = read_text(path, text)) die(*e);
    Corpus c;
    if (auto e = parse_corpus(text, path, c)) die(*e);
    return c;
}

/// Minimal argv parser: positionals plus --flag / --opt value.
class Args {
  public:
    Args(int argc, char** argv, const std::string& usage, const std::vector<std::string>& valued,
         const std::vector<std::string>& flags) {
        auto has = [](const std::vector<std::string>& v, const std::string& a) {
            return std::find(v.begin(), v.end(), a) != v.end();
        };
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--", 0) == 0) {
                if (has(valued, a)) {
                    if (i + 1 >= argc) die(a + " needs a value\nusage: " + usage);
                    opts_.emplace_back(a, std::string(argv[++i]));
                } else if (has(flags, a)) {
                    opts_.emplace_back(a, std::nullopt);
                } else {
                    die("unknown option " + a + "\nusage: " + usage);
                }
            } else {
                positional.push_back(a);
            }
        }
    }
    std::vector<std::string> positional;
    std::optional<std::string> get(const std::string& k) const {
        for (auto it = opts_.rbegin(); it != opts_.rend(); ++it)
            if (it->first == k) return it->second;
        return std::nullopt;
    }
    bool flag(const std::string& k) const {
        for (auto& o : opts_)
            if (o.first == k) return true;
        return false;
    }
    template <class T>
    T num(const std::string& k, T def) const {
        auto v = get(k);
        if (!v) return def;
        auto u = flat::parse_u64(*v);
        if (!u || *u > std::uint64_t(std::numeric_limits<T>::max())) die(k + ": not a number: " + *v);
        return T(*u);
    }

  private:
    std::vector<std::pair<std::string, std::optional<std::string>>> opts_;
};

inline const std::vector<std::string> COMMON_VALUED = {"--partitions", "--partition-map", "--journal-dir"};
inline const std::vector<std::string> COMMON_FLAGS = {"--binary"};

inline PartitionMap partition_map(const Args& a) {
    std::uint32_t p = a.num<std::uint32_t>("--partitions", 1);
    PartitionMap m;
    if (auto path = a.get("--partition-map")) {
        std::string text;
        if (auto e = read_text(*path, text)) die(*e);
        if (auto e = PartitionMap::parse_table(text, p, m)) die(*path + ": " + *e);
    } else if (auto e = PartitionMap::make(p, {}, m)) {
        die(*e);
    }
    return m;
}

struct Common {
    PartitionMap map;
    std::optional<JournalConfig> journal;
};

inline Common common(const Args& a) {
    Common c{partition_map(a), std::nullopt};
    if (a.flag("--binary") && !a.get("--journal-dir")) die("--binary requires --journal-dir");
    if (auto d = a.get("--journal-dir")) {
        JournalConfig j;
        j.dir = *d;
        j.format = a.flag("--binary") ? JournalFormat::Binary : JournalFormat::Jsonl;
        j.fsync = FsyncPolicy::never();  // harness runs need complete files, not power-loss safety
        j.events = true;
        c.journal = j;
    }
    return c;
}

/// spec/HARNESS.md §4.1 options beyond the common ones.
struct RunOpts {
    bool snapshot = false;
    std::optional<std::size_t> checkpoint_every;  // --checkpoint-every K (1.2)
    bool durable = false;                         // --durable (1.2)
};

/// Run a corpus through a fresh pipeline (one producer, file order), drain,
/// optionally snapshot, shut down. Returns the spec/HARNESS.md §3 listing.
template <MatchingCore C = FifoCore>
std::pair<std::string, std::optional<Snapshot>> run_corpus(const Corpus& corpus, const Common& c, bool tagged,
                                                           RunOpts opts) {
    const bool snapshot = opts.snapshot;
    auto [f, events] = collect(tagged);
    auto b = Pipeline<C>::builder();
    b.book_config(corpus.book).partition_map(c.map).egress(f);
    if (c.journal) {
        JournalConfig j = *c.journal;
        if (opts.durable) {
            j.fsync = FsyncPolicy::every_n(64);
            b.egress(acks([last = std::uint64_t(0)](std::uint32_t p, const EvtMsg& m) mutable {
                if (m.iseq != last) {
                    last = m.iseq;
                    std::fprintf(stderr, "acked %u %llu\n", p, (unsigned long long)m.iseq);
                    std::fflush(stderr);
                }
            }));
        }
        b.journal(j);
    } else if (opts.durable || opts.checkpoint_every) {
        die("--durable and --checkpoint-every need --journal-dir");
    }
    std::unique_ptr<Pipeline<C>> p;
    try {
        p = b.build();
    } catch (const std::exception& e) {
        die(e.what());
    }
    std::optional<Snapshot> snap;
    try {
        if (opts.checkpoint_every) {
            std::size_t k = *opts.checkpoint_every;
            if (k == 0) die("--checkpoint-every: K must be at least 1");
            for (std::size_t off = 0; off < corpus.cmds.size(); off += k) {
                std::size_t n = std::min(k, corpus.cmds.size() - off);
                if (p->publish_batch(corpus.cmds.data() + off, n) != Status::Ok) fail("pipeline closed");
                if (n == k) p->checkpoint();
            }
        } else if (p->publish_batch(corpus.cmds) != Status::Ok) {
            fail("pipeline closed");
        }
        p->drain();
        if (snapshot) snap = p->snapshot();
        p->shutdown();
    } catch (const std::exception& e) {
        fail(e.what());
    }
    return {events->listing(), snap};
}

inline void print(const std::string& s) {
    if (std::fwrite(s.data(), 1, s.size(), stdout) != s.size()) std::exit(0);
    std::fflush(stdout);
}

}  // namespace orderer::harness
