// Ring protocol tests (orderer-rust tests/ring.rs, ported): wrap,
// multi-producer integrity, batching, gating, try-publish CAS path, stalled
// producers, barrier dependencies, wait strategies, multicast. CI also runs
// this under ThreadSanitizer.
#include <orderer/disruptor.hpp>

#include <atomic>
#include <barrier>
#include <chrono>
#include <set>
#include <thread>
#include <tuple>

#include "check.hpp"

using namespace orderer::disruptor;
using namespace std::chrono_literals;

template <class T, class F>
static void drain_n(Consumer<T>& c, std::size_t n, F f) {
    auto deadline = std::chrono::steady_clock::now() + 20s;
    std::size_t got = 0;
    while (got < n) {
        got += c.poll(f);
        if (std::chrono::steady_clock::now() > deadline) { CHECK(false) << "timed out at " << got; return; }
        if (got < n) std::this_thread::yield();
    }
}

TEST(spsc_wrap) {
    RingBuilder<std::uint64_t> b(8);
    b.consumer();
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    const std::uint64_t N = 100000;
    std::thread reader([&, c = std::move(cs[0])]() mutable {
        std::uint64_t expect = 0;
        drain_n(c, N, [&](const std::uint64_t& v, std::int64_t seq, bool) {
            CHECK(v == expect && std::uint64_t(seq) == expect) << v;
            ++expect;
        });
    });
    for (std::uint64_t i = 0; i < N; ++i) p.publish([&](std::uint64_t& s) { s = i; });
    reader.join();
}

struct Msg {
    std::uint64_t producer = 0, counter = 0, check = 0;
};

static void multi_producer_run(std::size_t batch) {
    const std::uint64_t PRODUCERS = 4, PER = 100000;
    RingBuilder<Msg> b(1024);
    b.consumer();
    auto built = b.build_multi(); auto& p = built.first; auto& cs = built.second;
    std::thread reader([&, c = std::move(cs[0])]() mutable {
        std::vector<std::uint64_t> next(PRODUCERS, 0);
        std::int64_t last = -1;
        drain_n(c, PRODUCERS * PER, [&](const Msg& m, std::int64_t seq, bool) {
            CHECK(seq == last + 1) << "each seq once, in order";
            last = seq;
            CHECK(m.check == ((m.producer * 31) ^ m.counter)) << "torn slot";
            CHECK(m.counter == next[m.producer]) << "per-producer order";
            ++next[m.producer];
        });
    });
    std::vector<std::thread> ws;
    for (std::uint64_t id = 0; id < PRODUCERS; ++id)
        ws.emplace_back([&, id, pp = p]() mutable {
            for (std::uint64_t i = 0; i < PER;) {
                std::size_t n = std::min<std::uint64_t>(batch, PER - i);
                pp.publish_batch(n, [&](std::size_t k, Msg& s) { s = {id, i + k, (id * 31) ^ (i + k)}; });
                i += n;
            }
        });
    for (auto& w : ws) w.join();
    reader.join();
}

TEST(multi_producer_integrity_single_claims) { multi_producer_run(1); }
TEST(multi_producer_integrity_batched_claims) { multi_producer_run(37); }

TEST(batch_claim_consume_and_staging) {
    RingBuilder<std::uint32_t> b(16);
    b.consumer();
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    p.publish_batch(5, [](std::size_t i, std::uint32_t& s) { s = std::uint32_t(i * 10); });
    std::vector<std::tuple<std::uint32_t, std::int64_t, bool>> seen;
    CHECK(cs[0].poll([&](const std::uint32_t& v, std::int64_t s, bool e) { seen.emplace_back(v, s, e); }) == 5);
    CHECK(std::get<2>(seen.back()) && !std::get<2>(seen.front())) << "end_of_batch on the last only";
    p.stage([](std::uint32_t& s) { s = 1; });
    p.stage([](std::uint32_t& s) { s = 2; });
    CHECK(p.staged() == 2);
    CHECK(cs[0].poll([](auto&, auto, auto) {}) == 0) << "staged is invisible";
    p.commit();
    CHECK(cs[0].poll([](auto&, auto, auto) {}) == 2);
    p.publish_batch(10, [](std::size_t i, std::uint32_t& s) { s = std::uint32_t(i); });
    cs[0].set_max_batch(4);
    CHECK(cs[0].poll([](auto&, auto, auto) {}) == 4);
}

TEST(gating_blocks_and_try_publish_full) {
    RingBuilder<std::uint32_t> b(4);
    b.consumer();
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    for (std::uint32_t i = 0; i < 4; ++i) CHECK(p.try_publish([&](std::uint32_t& s) { s = i; }) == Publish::Ok);
    CHECK(p.try_publish([](std::uint32_t& s) { s = 99; }) == Publish::Full);
    std::atomic<bool> done{false};
    std::thread w([&] { p.publish([](std::uint32_t& s) { s = 4; }); done = true; });
    std::this_thread::sleep_for(50ms);
    CHECK(!done) << "producer must not lap the consumer";
    std::vector<std::uint32_t> seen;
    cs[0].poll([&](const std::uint32_t& v, auto, auto) { seen.push_back(v); });
    w.join();
    cs[0].poll([&](const std::uint32_t& v, auto, auto) { seen.push_back(v); });
    CHECK((seen == std::vector<std::uint32_t>{0, 1, 2, 3, 4})) << "zero loss under Block";
}

TEST(staged_work_is_committed_before_waiting) {
    RingBuilder<std::uint32_t> b(4);
    b.consumer();
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    std::vector<std::uint32_t> seen;
    std::thread r([&, c = std::move(cs[0])]() mutable {
        drain_n(c, 10, [&](const std::uint32_t& v, auto, auto) { seen.push_back(v); });
    });
    for (std::uint32_t i = 0; i < 10; ++i) p.stage([&](std::uint32_t& s) { s = i; });
    p.commit();
    r.join();
    CHECK(seen.size() == 10 && seen.back() == 9);
}

TEST(try_publish_cas_never_leaks_claims) {
    RingBuilder<std::uint32_t> b(8);
    b.consumer();
    auto built = b.build_multi(); auto& p = built.first; auto& cs = built.second;
    for (std::uint32_t i = 0; i < 8; ++i) p.try_publish([&](std::uint32_t& s) { s = i; });
    CHECK(p.try_publish([](std::uint32_t& s) { s = 99; }) == Publish::Full);
    CHECK(p.try_publish_batch(3, [](std::size_t, std::uint32_t& s) { s = 99; }) == Publish::Full);
    CHECK(p.control().published() == 7) << "a failed try leaves the cursor untouched";
    CHECK(cs[0].poll([](auto&, auto, auto) {}) == 8);
    CHECK(p.try_publish([](std::uint32_t& s) { s = 8; }) == Publish::Ok);
}

TEST(try_and_block_producers_never_double_claim) {
    const std::uint32_t PER = 20000;
    RingBuilder<std::uint64_t> b(64);
    b.consumer();
    auto built = b.build_multi(); auto& p = built.first; auto& cs = built.second;
    std::atomic<std::size_t> accepted{0};
    std::atomic<bool> stop{false};
    std::size_t delivered = 0;
    std::thread r([&, c = std::move(cs[0])]() mutable {
        std::set<std::uint64_t> seen;
        std::int64_t last = -1;
        for (;;) {
            bool stopping = stop.load();
            auto n = c.poll([&](const std::uint64_t& v, std::int64_t seq, bool) {
                CHECK(seq == last + 1);
                last = seq;
                CHECK(seen.insert(v).second) << "delivered twice";
            });
            if (n == 0) {
                if (stopping) break;
                std::this_thread::yield();
            }
        }
        delivered = seen.size();
    });
    std::vector<std::thread> ws;
    for (std::uint64_t id = 0; id < 4; ++id)
        ws.emplace_back([&, id, pp = p]() mutable {
            for (std::uint32_t i = 0; i < PER; ++i) {
                std::uint64_t v = (id << 32) | i;
                if (id % 2 == 0) { pp.publish([&](std::uint64_t& s) { s = v; }); ++accepted; }
                else if (pp.try_publish([&](std::uint64_t& s) { s = v; }) == Publish::Ok) ++accepted;
            }
        });
    for (auto& w : ws) w.join();
    stop = true;
    r.join();
    CHECK(delivered == accepted.load()) << delivered << " vs " << accepted.load();
}

TEST(stalled_producer_gates_only_its_own_slot) {
    RingBuilder<std::uint32_t> b(16);
    b.consumer();
    auto built = b.build_multi(); auto& p = built.first; auto& cs = built.second;
    p.publish([](std::uint32_t& s) { s = 0; });
    p.publish([](std::uint32_t& s) { s = 1; });
    std::barrier claimed(2), release(2);
    std::thread a([&, pa = p]() mutable {
        pa.publish([&](std::uint32_t& s) { claimed.arrive_and_wait(); release.arrive_and_wait(); s = 2; });
    });
    claimed.arrive_and_wait();
    p.publish([](std::uint32_t& s) { s = 3; });
    p.publish([](std::uint32_t& s) { s = 4; });
    std::vector<std::uint32_t> seen;
    cs[0].poll([&](const std::uint32_t& v, auto, auto) { seen.push_back(v); });
    CHECK((seen == std::vector<std::uint32_t>{0, 1})) << "consumer stops at the unpublished slot";
    release.arrive_and_wait();
    a.join();
    cs[0].poll([&](const std::uint32_t& v, auto, auto) { seen.push_back(v); });
    CHECK((seen == std::vector<std::uint32_t>{0, 1, 2, 3, 4}));
}

TEST(barrier_dependency_orders_stages) {
    const std::int64_t N = 200000;
    RingBuilder<std::int64_t> b(256);
    auto ida = b.consumer();
    b.consumer({ida});
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    auto a_seq = cs[0].sequence();
    std::thread ta([&, c = std::move(cs[0])]() mutable { drain_n(c, N, [](auto&, auto, auto) { cpu_relax(); }); });
    std::thread tb([&, c = std::move(cs[1])]() mutable {
        drain_n(c, N, [&](const std::int64_t& v, std::int64_t seq, bool) {
            CHECK(v == seq);
            CHECK(a_seq->get() >= seq) << "stage B overtook stage A at " << seq;
        });
    });
    for (std::int64_t i = 0; i < N; ++i) p.publish([&](std::int64_t& s) { s = i; });
    ta.join();
    tb.join();
}

TEST(wait_strategies_all_deliver) {
    for (auto w : {WaitStrategy::busy_spin(), WaitStrategy::yield(), WaitStrategy::backoff(), WaitStrategy::blocking()}) {
        RingBuilder<std::uint32_t> b(16);
        b.consumer({}, w);
        auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
        auto ctl = p.control();
        std::vector<std::uint32_t> seen;
        std::thread r([&, c = std::move(cs[0])]() mutable {
            while (c.wait_poll([&](const std::uint32_t& v, auto, auto) { seen.push_back(v); })) {}
        });
        for (std::uint32_t i = 0; i < 20; ++i) {
            p.publish([&](std::uint32_t& s) { s = i; });
            if (i % 5 == 0) std::this_thread::sleep_for(3ms);
        }
        while (ctl.consumed() < 19) std::this_thread::yield();
        ctl.alert();
        r.join();
        CHECK(seen.size() == 20 && seen.back() == 19) << int(w.kind);
    }
}

TEST(multicast_all_see_everything_slowest_gates) {
    RingBuilder<std::uint32_t> b(8);
    for (int i = 0; i < 3; ++i) b.consumer();
    auto built = b.build_single(); auto& p = built.first; auto& cs = built.second;
    for (std::uint32_t i = 0; i < 8; ++i) p.publish([&](std::uint32_t& s) { s = i; });
    CHECK(cs[0].poll([](auto&, auto, auto) {}) == 8);
    CHECK(cs[1].poll([](auto&, auto, auto) {}) == 8);
    CHECK(p.try_publish([](std::uint32_t& s) { s = 8; }) == Publish::Full) << "third consumer gates";
    CHECK(cs[2].poll([](auto&, auto, auto) {}) == 8);
    CHECK(p.try_publish([](std::uint32_t& s) { s = 8; }) == Publish::Ok);
}

int main() { return check::run_all(); }
