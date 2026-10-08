// recover.hpp — recovery (spec/JOURNAL.md §4–5): restore a snapshot into
// per-partition cores under the restoring pipeline's routing, then replay
// command journals merged by iseq.
#pragma once

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "core.hpp"
#include "journal.hpp"
#include "pipeline.hpp"
#include "routing.hpp"

namespace orderer {

struct RecoverError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// A snapshot body + its .meta sidecar (missing sidecar ⇒ cut 0, e.g. a
/// matcher snapshot).
inline Snapshot read_snapshot(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw RecoverError("snapshot: " + path.string() + ": cannot read");
    Snapshot s;
    s.body.assign(std::istreambuf_iterator<char>(in), {});
    auto mp = meta_path(path);
    if (fs::exists(mp)) {
        std::ifstream m(mp, std::ios::binary);
        std::string line;
        std::getline(m, line);
        if (flat::get_str(line, "format") != std::optional<std::string_view>("orderer-meta/1"))
            throw RecoverError("snapshot: " + mp.string() + ": bad sidecar");
        auto iseq = flat::get_u64(line, "iseq");
        if (!iseq) throw RecoverError("snapshot: " + mp.string() + ": bad sidecar");
        s.iseq = *iseq;
        s.partitions = std::uint32_t(flat::get_u64(line, "partitions").value_or(1));
    }
    return s;
}

/// Empty cores, or cores restored from `snap` (its header overrides `book`).
template <MatchingCore C>
std::pair<BookConfig, std::vector<C>> restore(BookConfig book, const PartitionMap& map, const Snapshot* snap) {
    std::vector<C> cores;
    if (!snap) {
        for (std::uint32_t p = 0; p < map.partitions(); ++p) cores.emplace_back(book);
        return {book, std::move(cores)};
    }
    ParsedSnapshot ps;
    if (auto e = parse_snapshot(snap->body, ps)) throw RecoverError("snapshot: " + *e);
    for (std::uint32_t p = 0; p < map.partitions(); ++p) cores.emplace_back(ps.cfg);
    std::set<Symbol> seen;
    for (const auto& b : ps.books) {
        if (!seen.insert(b.symbol).second)
            throw RecoverError("snapshot: book " + std::to_string(b.symbol) + " appears twice");
        if (auto e = cores[map.partition(b.symbol)].restore_book(b.symbol, b.seq, b.orders))
            throw RecoverError("snapshot: " + *e);
    }
    return {ps.cfg, std::move(cores)};
}

/// One iseq-ordered stream of records after `after`; iseqs must be disjoint.
inline std::vector<CmdRecord> merge_journals(std::vector<std::vector<CmdRecord>> parts, std::uint64_t after) {
    std::vector<CmdRecord> all;
    for (auto& p : parts)
        for (auto& r : p)
            if (r.iseq > after) all.push_back(r);
    std::sort(all.begin(), all.end(), [](const CmdRecord& a, const CmdRecord& b) { return a.iseq < b.iseq; });
    for (std::size_t i = 1; i < all.size(); ++i)
        if (all[i].iseq == all[i - 1].iseq)
            throw CorruptJournal("iseq " + std::to_string(all[i].iseq) + " appears in two partitions");
    return all;
}

template <MatchingCore C>
struct Recovery {
    BookConfig book;
    std::vector<C> cores;
    std::uint64_t snapshot_iseq = 0, last_iseq = 0, replayed = 0;
    Initial<C> into_initial() && { return Initial<C>{std::move(cores), last_iseq + 1}; }
};

/// Snapshot (optional) + every command journal in `dir` (optional), records
/// after the cut replayed in iseq order: emit(partition, symbol, seq, event).
/// The book config comes from the snapshot, else the journals, else `book`.
template <MatchingCore C, class F>
Recovery<C> recover(BookConfig book, const PartitionMap& map, const Snapshot* snap,
                    const std::optional<std::pair<fs::path, JournalFormat>>& journal, F&& emit) {
    std::optional<std::pair<JournalHeader, std::vector<std::vector<CmdRecord>>>> journals;
    if (journal) journals = read_cmd_dir(journal->first, journal->second);
    if (journals && !snap) book = journals->first.book;
    auto [cfg, cores] = restore<C>(book, map, snap);
    if (journals && !flat::same_book(journals->first.book, cfg))
        throw RecoverError("snapshot: snapshot and journal book configs differ");
    std::uint64_t cut = snap ? snap->iseq : 0;
    std::vector<CmdRecord> recs;
    if (journals) recs = merge_journals(std::move(journals->second), cut);
    Recovery<C> r;
    r.book = cfg;
    r.snapshot_iseq = cut;
    r.last_iseq = recs.empty() ? cut : std::max(cut, recs.back().iseq);
    r.replayed = recs.size();
    for (const auto& rec : recs) {
        std::uint32_t p = map.partition(rec.sym);
        cores[p].apply(rec.sym, rec.cmd, [&](Symbol s, std::uint64_t seq, const Event& ev) { emit(p, s, seq, ev); });
    }
    r.cores = std::move(cores);
    return r;
}

}  // namespace orderer
