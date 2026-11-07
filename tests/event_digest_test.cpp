// The event-chain digest (spec/REPLICATION.md 13.16): conformance vectors
// shared with the other languages, sensitivity, continuity across a snapshot
// restore, the documented limitation, and cost.
//
//   event_digest_test <vectors-dir>
//
// ORDERER_GEN_DIGEST_VECTORS=DIR writes the vector files instead of checking.
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include "check.hpp"
#include "orderer/cluster/pipeline_sm.hpp"
#include "orderer/harness.hpp"

using namespace orderer;
using namespace orderer::cluster;
using orderer::harness::Corpus;
using orderer::harness::load_corpus;
using Cmds = std::vector<std::pair<Symbol, Command>>;

static std::string VEC;

static std::string slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), {});
}

static Corpus corpus(const std::string& name) { return load_corpus(VEC + "/pipeline/" + name + ".cmd.jsonl"); }

/// Run `cmds` through a fresh pipeline and return the chains.
static std::vector<std::uint64_t> chains(BookConfig book, const Cmds& cmds, std::uint32_t partitions) {
    EventDigest d(partitions);
    auto p = Pipeline<FifoCore>::builder().book_config(book).partitions(partitions).egress(d.factory()).build();
    p->publish_batch(cmds);
    p->drain();
    auto v = d.values();
    p->shutdown();
    return v;
}

/// SHA-256 over tag, commands applied, partitions, chains.
static Digest combine(std::uint64_t iseq, const std::vector<std::uint64_t>& chains) {
    const char tag[] = "orderer-evdigest/1";
    std::vector<std::uint8_t> b(tag, tag + sizeof tag - 1);
    put_u64(b, iseq);
    put_u32(b, static_cast<std::uint32_t>(chains.size()));
    for (auto c : chains) put_u64(b, c);
    return sha256(b);
}

static std::string vector_text(std::uint64_t iseq, const std::vector<std::uint64_t>& chains) {
    std::ostringstream o;
    o << "orderer-evdigest/1\npartitions " << chains.size() << "\ncommands " << iseq << "\nchains";
    for (std::size_t i = 0; i < chains.size(); ++i) {
        char buf[20];
        std::snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(chains[i]));
        o << (i ? " " : " ") << buf;
    }
    o << "\ndigest " << hex(combine(iseq, chains)) << "\n";
    return o.str();
}

static PipelineSm<FifoCore> make_sm(std::uint32_t partitions, BookConfig book = BookConfig{}) {
    PartitionMap map;
    PartitionMap::make(partitions, {}, map);
    return PipelineSm<FifoCore>(book, map, [partitions, book](std::optional<Initial<FifoCore>> init, EventDigest d) {
        auto b = Pipeline<FifoCore>::builder();
        b.book_config(book).partitions(partitions).egress(d.factory()).timestamps(false);
        if (init) b.initial(std::move(*init));
        return b.build();
    });
}

static Entry batch_entry(const Cmds& cmds) {
    std::vector<std::uint8_t> payload;
    encode_batch(cmds.data(), cmds.size(), payload);
    return Entry{1, Bytes(std::move(payload))};
}

TEST(chains_match_the_shared_vectors) {
    const char* gen = std::getenv("ORDERER_GEN_DIGEST_VECTORS");
    int checked = 0;
    for (const char* name : {"multisymbol", "fuzz_s11"}) {
        Corpus c = corpus(name);
        for (std::uint32_t p : {1u, 2u, 4u}) {
            const std::string text = vector_text(c.cmds.size(), chains(c.book, c.cmds, p));
            const std::string file = std::string(name) + ".P" + std::to_string(p) + ".txt";
            if (gen) {
                std::ofstream(std::string(gen) + "/" + file, std::ios::binary) << text;
            } else {
                CHECK(text == slurp(VEC + "/replication-digest/" + file)) << file;
                ++checked;
            }
        }
    }
    if (!gen) {
        CHECK(checked == 6);
    }
}

TEST(chains_are_deterministic_and_sensitive_to_every_field) {
    Cmds base;
    for (std::uint64_t i = 1; i <= 40; ++i)
        base.emplace_back(Symbol(i % 5), Command::new_limit(i, i % 2 == 0 ? Side::Bid : Side::Ask, 100 + std::int64_t(i % 3), 1 + i % 4, Tif::Gtc));
    // Sweeping orders at the end trade against whatever rests, which is what
    // exposes resting prices and quantities in the event stream.
    for (Symbol s : {0u, 1u, 2u, 3u, 4u, 7u, 8u, 9u, 10u, 11u}) {
        base.emplace_back(s, Command::new_limit(9000 + s, Side::Bid, 1000, 10000, Tif::Ioc));
        base.emplace_back(s, Command::new_limit(9100 + s, Side::Ask, 0, 10000, Tif::Ioc));
    }
    BookConfig book{};
    const auto ref = chains(book, base, 2);
    CHECK(ref == chains(book, base, 2)) << "not deterministic";
    auto tweak = [&](auto f) {
        Cmds v = base;
        f(v[0]);
        return chains(book, v, 2);
    };
    CHECK(ref != tweak([](auto& c) { c.second.qty += 1; })) << "a different qty did not change the chain";
    CHECK(ref != tweak([](auto& c) { c.second.price += 1; })) << "a different price did not change the chain";
    CHECK(ref != tweak([](auto& c) { c.second.order_id += 1000; })) << "a different order id did not change the chain";
    CHECK(ref != tweak([](auto& c) { c.first += 7; })) << "a different symbol did not change the chain";
    Cmds shorter(base.begin(), base.end() - 1);
    CHECK(ref != chains(book, shorter, 2)) << "dropping a command did not change the chain";
}

TEST(a_restored_pipeline_continues_the_same_chain) {
    Corpus c = corpus("fuzz_s11");
    const std::uint32_t P = 2;
    const auto whole = chains(c.book, c.cmds, P);

    EventDigest d1(P);
    auto first = Pipeline<FifoCore>::builder().book_config(c.book).partitions(P).egress(d1.factory()).build();
    first->publish_batch(c.cmds.data(), 1500);
    first->drain();
    Snapshot snap = first->snapshot();
    const auto mid = d1.values();
    first->shutdown();

    PartitionMap map;
    CHECK(!PartitionMap::make(P, {}, map));
    auto [cfg, cores] = orderer::restore<FifoCore>(c.book, map, &snap);
    (void)cfg;
    EventDigest d2(P, mid);
    Initial<FifoCore> init;
    init.cores = std::move(cores);
    init.next_iseq = snap.iseq + 1;
    auto second = Pipeline<FifoCore>::builder().book_config(c.book).partition_map(map).egress(d2.factory()).initial(std::move(init)).build();
    second->publish_batch(c.cmds.data() + 1500, c.cmds.size() - 1500);
    second->drain();
    CHECK(d2.values() == whole) << "the chain broke across a snapshot";
    second->shutdown();
}

TEST(the_state_machine_snapshot_restores_chains_and_refuses_strangers) {
    Corpus c = corpus("fuzz_s11");
    auto a = make_sm(2);
    Cmds first(c.cmds.begin(), c.cmds.begin() + 1500), rest(c.cmds.begin() + 1500, c.cmds.end());
    a.apply(1, batch_entry(first));
    auto snap = a.snapshot(1);
    CHECK(snap.size() > 24 && std::memcmp(snap.data(), "PSM2", 4) == 0);
    a.apply(2, batch_entry(rest));

    auto b = make_sm(2);
    CHECK(!b.restore(1, snap.data(), snap.size())) << "restore failed";
    b.apply(2, batch_entry(rest));
    CHECK(a.digest(2) == b.digest(2)) << "restored replica's digest differs";
    CHECK(a.deep_digest(2) == b.deep_digest(2)) << "restored replica's deep digest differs";

    auto four = make_sm(4);
    CHECK(four.restore(1, snap.data(), snap.size()).has_value()) << "a snapshot with another partition count was accepted";
    std::vector<std::uint8_t> old(snap);
    std::memcpy(old.data(), "PSM1", 4);
    CHECK(make_sm(2).restore(1, old.data(), old.size()).has_value()) << "an unknown snapshot layout was accepted";
}

TEST(a_resting_difference_is_invisible_to_the_chain_until_it_trades) {
    // The limit of an event-stream digest, stated as a test: books that differ
    // only in a resting order's price emit the same events until something
    // trades against it. The deep digest sees it at once.
    auto run = [](std::int64_t price) {
        auto sm = make_sm(1);
        sm.apply(1, batch_entry({{1, Command::new_limit(1, Side::Ask, price, 5, Tif::Gtc)}}));
        return std::pair{*sm.digest(1), *sm.deep_digest(1)};
    };
    auto [chain_a, deep_a] = run(100);
    auto [chain_b, deep_b] = run(101);
    CHECK(chain_a == chain_b) << "the chain saw a price that has not traded";
    CHECK(deep_a != deep_b) << "the deep digest missed a different resting price";
}

TEST(the_digest_costs_a_barrier_not_a_snapshot) {
    auto sm = make_sm(2);
    // 300k resting orders over 8 symbols: bids and asks that never cross.
    Cmds cmds;
    for (std::uint64_t i = 1; i <= 300000; ++i) {
        const bool bid = i % 2 == 0;
        cmds.emplace_back(Symbol(i % 8), Command::new_limit(i, bid ? Side::Bid : Side::Ask, bid ? 1000 - std::int64_t(i % 500) : 2000 + std::int64_t(i % 500), 1, Tif::Gtc));
    }
    sm.apply(1, batch_entry(cmds));
    using Clock = std::chrono::steady_clock;
    auto t = Clock::now();
    auto cheap = sm.digest(1);
    const auto cheap_t = Clock::now() - t;
    t = Clock::now();
    auto deep = sm.deep_digest(1);
    const auto deep_t = Clock::now() - t;
    const auto us = [](auto d) { return std::chrono::duration_cast<std::chrono::microseconds>(d).count(); };
    std::cout << "  digest " << us(cheap_t) << " us, deep digest " << us(deep_t) << " us (300k resting orders)\n";
    CHECK(cheap != deep);
    CHECK(cheap_t < deep_t / 10) << "digest " << us(cheap_t) << " us is not much cheaper than deep " << us(deep_t) << " us";
    CHECK(us(cheap_t) < 100000) << "digest took " << us(cheap_t) << " us";
}

int main(int argc, char** argv) {
    VEC = argc > 1 ? argv[1] : "vectors";
    return check::run_all();
}
