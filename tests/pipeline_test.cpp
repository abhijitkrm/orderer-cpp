// Integration suite (orderer-rust tests/{partitions,journal_recovery,
// control,plugs}.rs, ported).
//   pipeline_test <vectors-dir> <scratch-dir>
#include <atomic>
#include <thread>

#include "common.hpp"

using namespace t;
using namespace std::chrono_literals;

static fs::path SCRATCH;

// ---- partitions ---------------------------------------------------------------------------

TEST(every_partition_count_matches_reference_per_symbol) {
    auto cfg = fuzz_cfg();
    for (std::uint64_t seed = 1; seed <= 8; ++seed) {
        auto cmds = fuzz_corpus(seed, 4000, 8);
        auto ref = reference_lines(cfg, cmds);
        for (std::uint32_t P : {1u, 2u, 3u, 4u, 7u}) {
            auto parts = run_pipeline(cfg, cmds, P, true);
            if (P == 1) {
                CHECK(parts[0] == ref) << "seed " << seed << ": P=1 is the plain engine stream";
            }
            auto all = concat(parts);
            CHECK(dense(all)) << "seq density";
            CHECK(by_symbol(all) == by_symbol(ref)) << "seed " << seed << " P=" << P;
            for (std::uint32_t q = 0; q < parts.size(); ++q)
                for (auto& l : parts[q]) CHECK(hash_partition(Symbol(*flat::get_u64(l, "symbol")), P) == q);
        }
    }
}

TEST(fuzz_runs_are_deterministic_per_partition) {
    for (std::uint64_t seed = 1; seed <= 8; ++seed) {
        auto cmds = fuzz_corpus(seed, 3000, 8);
        CHECK(run_pipeline(fuzz_cfg(), cmds, 4, true) == run_pipeline(fuzz_cfg(), cmds, 4, true)) << seed;
    }
}

TEST(partition_table_routes_symbols) {
    auto cmds = fuzz_corpus(11, 3000, 8);
    std::vector<std::pair<std::uint32_t, std::uint32_t>> table;
    for (std::uint32_t s = 0; s < 8; ++s) table.emplace_back(s, s == 5 ? 0 : 2);
    PartitionMap m;
    CHECK(!PartitionMap::make(3, table, m));
    auto [f, h] = collect(true);
    auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).partition_map(m).egress(f).build();
    p->publish_batch(cmds);
    p->drain();
    p->shutdown();
    std::vector<std::vector<std::string>> parts;
    for (auto& b : h->take()) parts.push_back(lines(b));
    CHECK(parts[1].empty());
    for (auto& l : parts[0]) CHECK(*flat::get_u64(l, "symbol") == 5);
    CHECK(by_symbol(concat(parts)) == by_symbol(reference_lines(fuzz_cfg(), cmds)));
}

TEST(many_producers_preserve_per_symbol_order) {
    auto cmds = fuzz_corpus(3, 20000, 16);
    auto [f, h] = collect(true);
    auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).partitions(4).ring_sizes(256, 64, 64).egress(f).build();
    std::vector<std::thread> ts;
    for (std::uint32_t k = 0; k < 4; ++k) {
        Cmds mine;
        for (auto& c : cmds)
            if (c.first % 4 == k) mine.push_back(c);
        ts.emplace_back([mine, hd = p->handle()]() mutable {
            for (std::size_t i = 0; i < mine.size(); i += 7) {
                std::size_t n = std::min<std::size_t>(7, mine.size() - i);
                if (n % 2 == 0) hd.publish_batch(mine.data() + i, n);
                else for (std::size_t j = 0; j < n; ++j) hd.publish(mine[i + j].first, mine[i + j].second);
            }
        });
    }
    for (auto& t : ts) t.join();
    p->drain();
    p->shutdown();
    CHECK(by_symbol(lines(h->listing())) == by_symbol(reference_lines(fuzz_cfg(), cmds)));
}

// ---- journals + recovery ------------------------------------------------------------------

static JournalConfig jcfg(const fs::path& d, JournalFormat f) {
    JournalConfig j;
    j.dir = d;
    j.format = f;
    j.fsync = FsyncPolicy::every_n(64);
    return j;
}

TEST(journals_snapshot_and_recovery_round_trip) {
    auto cfg = fuzz_cfg();
    for (auto fmt : {JournalFormat::Jsonl, JournalFormat::Binary}) {
        for (std::uint32_t P : {1u, 3u}) {
            auto dir = scratch(SCRATCH, "jr-" + std::to_string(int(fmt)) + "-" + std::to_string(P));
            auto cmds = fuzz_corpus(21 + P, 5000, 8);
            const std::size_t cut = 2000;
            auto [f, h] = collect(true);
            auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(P).ring_sizes(512, 128, 128)
                         .journal(jcfg(dir, fmt)).egress(f).build();
            Cmds first(cmds.begin(), cmds.begin() + cut), second(cmds.begin() + cut, cmds.end());
            p->publish_batch(first);
            auto snap = p->snapshot();
            p->publish_batch(second);
            p->shutdown();
            auto all_ref = reference_lines(cfg, cmds);
            auto prefix_len = reference_lines(cfg, first).size();
            CHECK(snap.iseq == cut);
            CHECK(snap.body == reference_snapshot(cfg, cmds, cut)) << "snapshot body";
            auto [hdr, recs] = read_cmd_dir(dir, fmt);
            CHECK(hdr.partitions == P && flat::same_book(hdr.book, cfg));
            std::vector<CmdRecord> merged;
            for (std::uint32_t q = 0; q < recs.size(); ++q)
                for (auto& r : recs[q]) { CHECK(hash_partition(r.sym, P) == q); merged.push_back(r); }
            std::sort(merged.begin(), merged.end(), [](auto& a, auto& b) { return a.iseq < b.iseq; });
            CHECK(merged.size() == cmds.size());
            for (std::size_t i = 0; i < merged.size() && i < cmds.size(); ++i)
                CHECK(merged[i].iseq == i + 1 && merged[i].sym == cmds[i].first) << i;
            auto collected = h->take();
            for (std::uint32_t q = 0; q < P; ++q)
                CHECK(read_evt_journal(journal_path(dir, Kind::Evt, q, fmt), fmt) == lines(collected[q])) << "evt-" << q;
            for (std::uint32_t rp : {P, 2u}) {
                PartitionMap m;
                PartitionMap::make(rp, {}, m);
                std::vector<std::string> replayed;
                auto rec = recover<FifoCore>(cfg, m, &snap, std::make_optional(std::make_pair(dir, fmt)),
                                             [&](std::uint32_t, Symbol s, std::uint64_t q, const Event& e) {
                                                 std::string l;
                                                 write_canonical_sym(q, s, e, l);
                                                 replayed.push_back(l);
                                             });
                CHECK(rec.snapshot_iseq == cut && rec.last_iseq == cmds.size() && rec.replayed == cmds.size() - cut);
                CHECK(std::vector<std::string>(all_ref.begin() + std::ptrdiff_t(prefix_len), all_ref.end()) == replayed)
                    << "recover P=" << P << " → " << rp;
            }
        }
    }
}

TEST(recovered_pipeline_resumes_and_appends) {
    auto cfg = fuzz_cfg();
    auto dir = scratch(SCRATCH, "resume");
    auto j = jcfg(dir, JournalFormat::Binary);
    auto cmds = fuzz_corpus(77, 4000, 6);
    Cmds first(cmds.begin(), cmds.begin() + 2500), second(cmds.begin() + 2500, cmds.end());
    {
        auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(2).journal(j).build();
        p->publish_batch(first);
        p->shutdown();
    }
    PartitionMap m;
    PartitionMap::make(2, {}, m);
    auto rec = recover<FifoCore>(cfg, m, nullptr, std::make_optional(std::make_pair(dir, JournalFormat::Binary)),
                                 [](auto, auto, auto, auto&) {});
    CHECK(rec.last_iseq == 2500);
    j.append = true;
    auto [f, h] = collect(true);
    auto book = rec.book;
    auto p = Pipeline<FifoCore>::builder().book_config(book).partition_map(m).journal(j).egress(f)
                 .initial(std::move(rec).into_initial()).build();
    p->publish_batch(second);
    p->drain();
    auto snap = p->snapshot();
    p->shutdown();
    auto all_ref = reference_lines(cfg, cmds);
    auto prefix_len = reference_lines(cfg, first).size();
    CHECK(by_symbol(lines(h->listing())) ==
          by_symbol(std::vector<std::string>(all_ref.begin() + std::ptrdiff_t(prefix_len), all_ref.end())));
    CHECK(snap.iseq == 4000) << "iseq resumed";
    CHECK(snap.body == reference_snapshot(cfg, cmds, cmds.size()));
    auto [hdr, recs] = read_cmd_dir(dir, JournalFormat::Binary);
    std::size_t total = 0;
    for (auto& r : recs) total += r.size();
    CHECK(total == 4000) << "appended journals hold the whole history";
}

TEST(torn_and_corrupt_journals_are_errors) {
    auto cmds = fuzz_corpus(8, 500, 1);
    for (auto fmt : {JournalFormat::Jsonl, JournalFormat::Binary}) {
        auto dir = scratch(SCRATCH, "torn-" + std::to_string(int(fmt)));
        {
            auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).journal(jcfg(dir, fmt)).build();
            p->publish_batch(cmds);
            p->shutdown();
        }
        auto path = journal_path(dir, Kind::Cmd, 0, fmt);
        std::string good = slurp(path);
        auto expect_corrupt = [&](const std::string& bytes, const std::string& what) {
            std::ofstream(path, std::ios::binary) << bytes;
            bool threw = false;
            try { read_cmd_dir(dir, fmt); } catch (const CorruptJournal& e) {
                threw = true;
                CHECK(std::string(e.what()).find(what) != std::string::npos) << e.what();
            }
            CHECK(threw) << what;
        };
        expect_corrupt(good.substr(0, good.size() - 7), "torn");
        std::string back = good;
        if (fmt == JournalFormat::Binary) {
            // a well-formed (resealed) record, just out of order
            auto off = back.size() - CMD_RECORD;
            for (int i = 0; i < 8; ++i) back[off + std::size_t(i)] = i == 0 ? 1 : 0;
            seal(reinterpret_cast<std::uint8_t*>(&back[off]), CMD_RECORD_V1);
        } else {
            back += "{\"cmd\":\"cancel\",\"symbol\":0,\"order_id\":1,\"iseq\":3}\n";
        }
        expect_corrupt(back, "iseq");
        std::string hdr = good;
        hdr[2] = '#';
        expect_corrupt(hdr, "");
        std::ofstream(path, std::ios::binary) << good;
        try { read_cmd_dir(dir, fmt); } catch (...) { CHECK(false) << "good journal rejected"; }
    }
}

// ---- controls -------------------------------------------------------------------------------

TEST(snapshots_under_load_are_clean_cuts) {
    auto cfg = fuzz_cfg();
    auto cmds = fuzz_corpus(31, 30000, 8);
    auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(3).ring_sizes(256, 64, 64).build();
    std::thread prod([&, h = p->handle()]() mutable {
        for (std::size_t i = 0; i < cmds.size(); i += 50) h.publish_batch(cmds.data() + i, std::min<std::size_t>(50, cmds.size() - i));
    });
    std::vector<Snapshot> snaps;
    while (snaps.size() < 6) { snaps.push_back(p->snapshot()); std::this_thread::sleep_for(2ms); }
    prod.join();
    snaps.push_back(p->snapshot());
    p->shutdown();
    for (auto& s : snaps) CHECK(s.body == reference_snapshot(cfg, cmds, s.iseq)) << "cut at " << s.iseq;
    CHECK(snaps.back().iseq == cmds.size());
}

TEST(shutdown_is_idempotent_and_closes_publishing) {
    auto p = Pipeline<FifoCore>::builder().partitions(2).build();
    auto h = p->handle();
    p->publish(1, Command::new_limit(1, Side::Bid, 10, 1, Tif::Gtc));
    p->shutdown();
    p->shutdown();
    CHECK(p->publish(1, Command::cancel(1)) == Status::Closed);
    CHECK(h.publish(1, Command::cancel(1)) == Status::Closed);
    CHECK(h.try_publish(1, Command::cancel(1)) == Status::Closed);
    bool threw = false;
    try { p->drain(); } catch (const Error& e) { threw = e.kind == Error::Kind::Closed; }
    CHECK(threw) << "drain after shutdown";
}

TEST(every_ok_publish_racing_shutdown_is_applied) {
    for (int round = 0; round < 5; ++round) {
        auto [f, h] = collect(true);
        auto p = Pipeline<NoopCore>::builder().partitions(2).ring_sizes(64, 16, 16).egress(f).build();
        std::atomic<std::size_t> accepted{0};
        std::vector<std::thread> ts;
        for (std::uint32_t k = 0; k < 3; ++k)
            ts.emplace_back([&, k, hd = p->handle()]() mutable {
                for (std::uint64_t i = 0; hd.publish(k, Command::cancel(i)) == Status::Ok; ++i) ++accepted;
            });
        std::this_thread::sleep_for(5ms);
        p->shutdown();
        for (auto& t : ts) t.join();
        CHECK(lines(h->listing()).size() == accepted.load()) << "Ok ⇒ applied";
    }
}

TEST(acks_wait_for_fsync) {
    auto cmds = fuzz_corpus(4, 2000, 6);
    auto total = reference_lines(fuzz_cfg(), cmds).size();
    auto run = [&](FsyncPolicy pol, bool expect_none_before_shutdown) {
        auto dir = scratch(SCRATCH, "acks");
        auto j = jcfg(dir, JournalFormat::Binary);
        j.fsync = pol;
        j.events = false;
        auto acked = std::make_shared<std::atomic<std::size_t>>(0);
        auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).partitions(2).journal(j)
                     .egress(acks([acked](std::uint32_t, const EvtMsg&) { ++*acked; })).build();
        p->publish_batch(cmds);
        p->drain();
        if (expect_none_before_shutdown) {
            std::this_thread::sleep_for(100ms);
            CHECK(*acked == 0) << "acked before any fsync";
            CHECK(p->durable_iseq(0) == 0 && p->durable_iseq(1) == 0);
        } else {
            auto deadline = std::chrono::steady_clock::now() + 20s;
            while (*acked < total && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(5ms);
            CHECK(std::max(p->durable_iseq(0), p->durable_iseq(1)) == cmds.size());
        }
        p->shutdown();
        CHECK(*acked == total);
    };
    run(FsyncPolicy::every(std::chrono::hours(1)), true);
    auto idle = FsyncPolicy::every_n(1ull << 40);
    idle.idle = std::chrono::milliseconds(20);
    run(idle, false);
}

struct Slow final : Egress {
    void on_event(const EvtMsg&) override { std::this_thread::sleep_for(20us); }
};

TEST(tiny_rings_and_slow_egress_block_without_loss) {
    auto cmds = fuzz_corpus(17, 3000, 4);
    auto [f, h] = collect(true);
    auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).partitions(2).ring_sizes(2, 2, 2)
                 .egress([](const EgressCtx&) -> std::unique_ptr<Egress> { return std::make_unique<Slow>(); })
                 .egress(f).build();
    for (auto& [s, c] : cmds) CHECK(p->publish(s, c) == Status::Ok);
    p->drain();
    p->shutdown();
    CHECK(by_symbol(lines(h->listing())) == by_symbol(reference_lines(fuzz_cfg(), cmds)));
}

TEST(try_publish_sheds_at_the_edge_only) {
    auto [f, h] = collect(true);
    auto p = Pipeline<NoopCore>::builder().partitions(1).ring_sizes(4, 2, 2)
                 .egress([](const EgressCtx&) -> std::unique_ptr<Egress> { return std::make_unique<Slow>(); })
                 .egress(f).build();
    std::size_t ok = 0, full = 0;
    for (std::uint64_t i = 0; i < 2000; ++i) {
        auto s = p->try_publish(1, Command::cancel(i));
        if (s == Status::Ok) ++ok;
        else if (s == Status::Full) ++full;
    }
    p->drain();
    p->shutdown();
    CHECK(full > 0);
    auto got = lines(h->listing());
    CHECK(got.size() == ok && dense(got));
}

// ---- plugs ------------------------------------------------------------------------------------

TEST(noop_core_sees_every_command) {
    auto cmds = fuzz_corpus(5, 5000, 8);
    auto got = concat(run_pipeline<NoopCore>(fuzz_cfg(), cmds, 3, true));
    CHECK(got.size() == cmds.size() && dense(got));
}

/// A core that throws on a poison order id.
struct PanicCore {
    FifoCore inner;
    explicit PanicCore(BookConfig c) : inner(c) {}
    template <class F>
    void apply(Symbol s, const Command& c, F&& emit) {
        if (c.kind == Command::Kind::Cancel && c.order_id == 666) throw std::runtime_error("poison command");
        inner.apply(s, c, emit);
    }
    void snapshot_blocks(std::vector<Block>& b) { inner.snapshot_blocks(b); }
    std::optional<std::string> restore_book(Symbol s, std::uint64_t q, const std::vector<RestingOrder>& o) {
        return inner.restore_book(s, q, o);
    }
};
static_assert(MatchingCore<PanicCore>);

TEST(failing_core_fails_the_pipeline_instead_of_hanging) {
    auto p = Pipeline<PanicCore>::builder().partitions(2).build();
    p->publish(1, Command::new_limit(1, Side::Bid, 10, 1, Tif::Gtc));
    p->publish(1, Command::cancel(666));
    bool failed = false;
    try { p->drain(); } catch (const Error& e) {
        failed = e.kind == Error::Kind::Failed && std::string(e.what()).find("engine") != std::string::npos;
    }
    CHECK(failed);
    failed = false;
    try { p->shutdown(); } catch (const Error& e) { failed = e.kind == Error::Kind::Failed; }
    CHECK(failed);
}

// ---- 1.2: checksums, repair, checkpoints --------------------------------------------------

TEST(crc32c_matches_the_spec_check_value) {
    const char* v = "123456789";
    CHECK(crc32c(reinterpret_cast<const std::uint8_t*>(v), 9) == 0xE3069283u);
}

TEST(checksums_catch_flipped_bits_anywhere) {
    auto dir = scratch(SCRATCH, "crc");
    auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).journal(jcfg(dir, JournalFormat::Binary)).build();
    p->publish_batch(fuzz_corpus(9, 300, 2));
    p->shutdown();
    auto path = journal_path(dir, Kind::Cmd, 0, JournalFormat::Binary);
    std::string bad = slurp(path);
    bad[HEADER + 100 * CMD_RECORD + 20] ^= 0x10;
    std::ofstream(path, std::ios::binary) << bad;
    bool threw = false;
    try { read_cmd_dir(dir, JournalFormat::Binary); } catch (const CorruptJournal& e) {
        threw = std::string(e.what()).find("checksum") != std::string::npos;
    }
    CHECK(threw) << "strict";
    threw = false;
    try { repair_dir(dir, JournalFormat::Binary); } catch (const CorruptJournal&) { threw = true; }
    CHECK(threw) << "mid-file damage is not repairable";
}

TEST(repair_cuts_only_a_torn_tail) {
    auto cmds = fuzz_corpus(10, 400, 3);
    for (auto fmt : {JournalFormat::Jsonl, JournalFormat::Binary}) {
        auto dir = scratch(SCRATCH, "repair-" + std::to_string(int(fmt)));
        auto p = Pipeline<FifoCore>::builder().book_config(fuzz_cfg()).journal(jcfg(dir, fmt)).build();
        p->publish_batch(cmds);
        p->shutdown();
        auto path = journal_path(dir, Kind::Cmd, 0, fmt);
        std::string good = slurp(path);
        auto full = read_cmd_dir(dir, fmt).second[0];
        std::ofstream(path, std::ios::binary) << good.substr(0, good.size() - 5);
        bool threw = false;
        try { read_cmd_dir(dir, fmt); } catch (const CorruptJournal&) { threw = true; }
        CHECK(threw) << "strict rejects a torn tail";
        CHECK(repair_dir(dir, fmt).size() == 1);
        auto got = read_cmd_dir(dir, fmt).second[0];
        CHECK(got.size() == full.size() - 1 && got.back().iseq == full[full.size() - 2].iseq) << "a prefix survives";
        if (fmt == JournalFormat::Binary) {
            std::string zeroed = good;
            std::fill(zeroed.end() - CMD_RECORD, zeroed.end(), '\0');
            std::ofstream(path, std::ios::binary) << zeroed;
            CHECK(repair_dir(dir, fmt).size() == 1) << "a complete record that never reached the disk";
            CHECK(read_cmd_dir(dir, fmt).second[0].size() == full.size() - 1);
        }
        CHECK(repair_dir(dir, fmt).empty()) << "a clean file is left alone";
    }
}

TEST(checkpoints_rotate_segments_and_bound_recovery) {
    auto cfg = fuzz_cfg();
    auto cmds = fuzz_corpus(12, 3000, 6);
    for (auto fmt : {JournalFormat::Jsonl, JournalFormat::Binary}) {
        auto dir = scratch(SCRATCH, "ckpt-" + std::to_string(int(fmt)));
        auto [f, h] = collect(true);
        auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(3).journal(jcfg(dir, fmt)).egress(f).build();
        Cmds a(cmds.begin(), cmds.begin() + 1000), b(cmds.begin() + 1000, cmds.begin() + 2200),
            c(cmds.begin() + 2200, cmds.end());
        p->publish_batch(a);
        auto c1 = p->checkpoint();
        p->publish_batch(b);
        auto c2 = p->checkpoint();
        p->publish_batch(c);
        p->shutdown();
        CHECK(c1.iseq == 1000 && c2.iseq == 2200);
        auto cps = list_checkpoints(dir);
        CHECK(cps.size() == 1 && cps[0].first == 2200) << "only the last checkpoint remains";
        for (Kind k : {Kind::Cmd, Kind::Evt}) {
            auto segs = list_segments(dir, k, fmt);
            CHECK(segs.size() == 3);
            for (auto& s : segs) CHECK(s.start == 2200) << s.path;
        }
        CHECK(c2.body == reference_snapshot(cfg, cmds, 2200));
        auto snap = read_snapshot(cps[0].second);
        PartitionMap m;
        PartitionMap::make(3, {}, m);
        std::vector<std::string> replayed;
        auto rec = recover<FifoCore>(cfg, m, &snap, std::make_optional(std::make_pair(dir, fmt)),
                                     [&](std::uint32_t, Symbol s, std::uint64_t q, const Event& e) {
                                         std::string l;
                                         write_canonical_sym(q, s, e, l);
                                         replayed.push_back(l);
                                     });
        auto all = reference_lines(cfg, cmds);
        auto prefix = reference_lines(cfg, Cmds(cmds.begin(), cmds.begin() + 2200)).size();
        CHECK(rec.replayed == cmds.size() - 2200);
        CHECK(std::vector<std::string>(all.begin() + std::ptrdiff_t(prefix), all.end()) == replayed);
        std::vector<std::string> evts;
        for (std::uint32_t q = 0; q < 3; ++q) {
            auto e = read_evt_partition(dir, fmt, q);
            evts.insert(evts.end(), e.begin(), e.end());
        }
        std::vector<std::string> want(all.begin() + std::ptrdiff_t(prefix), all.end());
        std::sort(evts.begin(), evts.end());
        std::sort(want.begin(), want.end());
        CHECK(evts == want) << "event segments hold the tail";
        CHECK(lines(h->listing()).size() == all.size());
    }
}

TEST(append_continues_the_last_segment_after_a_checkpoint) {
    auto cfg = fuzz_cfg();
    auto cmds = fuzz_corpus(14, 2000, 4);
    auto dir = scratch(SCRATCH, "ckpt-append");
    auto j = jcfg(dir, JournalFormat::Binary);
    {
        auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(2).journal(j).build();
        p->publish_batch(Cmds(cmds.begin(), cmds.begin() + 800));
        p->checkpoint();
        p->publish_batch(Cmds(cmds.begin() + 800, cmds.begin() + 1200));
        p->shutdown();
    }
    PartitionMap m;
    PartitionMap::make(2, {}, m);
    auto snap = read_snapshot(list_checkpoints(dir)[0].second);
    auto rec = recover<FifoCore>(cfg, m, &snap, std::make_optional(std::make_pair(dir, JournalFormat::Binary)),
                                 [](auto, auto, auto, auto&) {});
    CHECK(rec.last_iseq == 1200);
    j.append = true;
    auto book = rec.book;
    auto p = Pipeline<FifoCore>::builder().book_config(book).partition_map(m).journal(j)
                 .initial(std::move(rec).into_initial()).build();
    p->publish_batch(Cmds(cmds.begin() + 1200, cmds.end()));
    auto s = p->snapshot();
    p->shutdown();
    CHECK(s.iseq == 2000 && s.body == reference_snapshot(cfg, cmds, cmds.size()));
    std::size_t total = 0;
    for (auto& r : read_cmd_dir(dir, JournalFormat::Binary).second) total += r.size();
    CHECK(total == 1200) << "800 checkpointed away";
}

TEST(automatic_checkpoints_keep_the_directory_recoverable) {
    auto cfg = fuzz_cfg();
    auto cmds = fuzz_corpus(15, 20000, 8);
    auto dir = scratch(SCRATCH, "ckpt-auto");
    Snapshot last;
    {
        auto p = Pipeline<FifoCore>::builder().book_config(cfg).partitions(3).journal(jcfg(dir, JournalFormat::Binary))
                     .checkpoint_every(std::chrono::milliseconds(20)).build();
        for (std::size_t i = 0; i < cmds.size(); i += 500) {
            p->publish_batch(cmds.data() + i, std::min<std::size_t>(500, cmds.size() - i));
            std::this_thread::sleep_for(3ms);
        }
        last = p->snapshot();
        p->shutdown();
    }
    auto cps = list_checkpoints(dir);
    CHECK(cps.size() == 1 && cps[0].first > 0) << "one automatic checkpoint remains";
    auto snap = read_snapshot(cps[0].second);
    PartitionMap m;
    PartitionMap::make(3, {}, m);
    auto rec = recover<FifoCore>(cfg, m, &snap, std::make_optional(std::make_pair(dir, JournalFormat::Binary)),
                                 [](auto, auto, auto, auto&) {});
    CHECK(rec.last_iseq == cmds.size());
    auto book = rec.book;
    auto p2 = Pipeline<FifoCore>::builder().book_config(book).partition_map(m).initial(std::move(rec).into_initial()).build();
    CHECK(p2->snapshot().body == last.body) << "the directory recovers the final state";
    p2->shutdown();
    bool threw = false;
    try { Pipeline<FifoCore>::builder().checkpoint_every(std::chrono::milliseconds(5)).build(); }
    catch (const Error& e) { threw = e.kind == Error::Kind::Config; }
    CHECK(threw) << "needs journals";
}

int main(int argc, char** argv) {
    SCRATCH = argc > 2 ? argv[2] : "build/scratch";
    fs::create_directories(SCRATCH);
    return check::run_all();
}
