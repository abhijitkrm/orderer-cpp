// A5 — zero steady-state allocation. Global operator new is replaced with a
// counting version; after a warmup that creates every book and fills every
// pool, a million commands through a P=2 pipeline with binary command
// journals must allocate nothing on any thread.
//   no_alloc_test <scratch-dir>
#include <atomic>
#include <cstdlib>
#include <new>

#include "common.hpp"

static std::atomic<bool> counting{false};
static std::atomic<std::uint64_t> allocs{0};

static void* counted(std::size_t n) {
    if (counting.load(std::memory_order_relaxed)) allocs.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
static void* counted_aligned(std::size_t n, std::align_val_t al) {
    if (counting.load(std::memory_order_relaxed)) allocs.fetch_add(1, std::memory_order_relaxed);
    std::size_t a = std::max<std::size_t>(std::size_t(al), sizeof(void*));
    void* p = nullptr;
    if (posix_memalign(&p, a, n ? n : a) != 0) throw std::bad_alloc();
    return p;
}
void* operator new(std::size_t n) { return counted(n); }
void* operator new[](std::size_t n) { return counted(n); }
void* operator new(std::size_t n, std::align_val_t a) { return counted_aligned(n, a); }
void* operator new[](std::size_t n, std::align_val_t a) { return counted_aligned(n, a); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

static std::filesystem::path SCRATCH;

/// Non-crossing churn on 16 books plus crossing IOCs: every hot path.
static t::Cmds workload(std::size_t n, std::uint64_t seed) {
    t::Rng r{seed | 1};
    t::Cmds out;
    for (std::size_t i = 0; i < n; ++i) {
        orderer::Symbol sym = orderer::Symbol(r.next() % 16);
        std::uint64_t id = r.next() % 512;
        bool bid = r.next() % 2 == 0;
        std::int64_t px = bid ? 900 + std::int64_t(r.next() % 100) : 1001 + std::int64_t(r.next() % 100);
        std::uint64_t qty = r.next() % 50 + 1;
        orderer::Side side = bid ? orderer::Side::Bid : orderer::Side::Ask;
        orderer::Command c;
        switch (r.next() % 10) {
            case 0: case 1: case 2: case 3: c = orderer::Command::new_limit(id, side, px, qty, orderer::Tif::Gtc); break;
            case 4: c = orderer::Command::new_limit(id, side, bid ? 1050 : 950, qty, orderer::Tif::Ioc); break;
            case 5: case 6: c = orderer::Command::cancel(id); break;
            default: c = orderer::Command::replace(id, px, qty);
        }
        out.emplace_back(sym, c);
    }
    return out;
}

TEST(steady_state_allocates_nothing) {
    using namespace orderer;
    auto dir = t::scratch(SCRATCH, "no-alloc");
    JournalConfig j;
    j.dir = dir;
    j.format = JournalFormat::Binary;
    j.fsync = FsyncPolicy::every_n(1024);
    j.events = false;
    auto p = Pipeline<FifoCore>::builder().book_config(BookConfig{1, 2000, 1024, IndexKind::Ladder})
                 .partitions(2).journal(j).build();
    auto warm = workload(300000, 7), measured = workload(1000000, 8);
    p->publish_batch(warm);
    p->drain();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto h = p->handle();  // a Handle registers its in-flight flag once, here
    counting = true;
    for (std::size_t i = 0; i < measured.size(); i += 64)
        h.publish_batch(measured.data() + i, std::min<std::size_t>(64, measured.size() - i));
    p->drain();
    counting = false;
    p->shutdown();
    CHECK(allocs.load() == 0) << allocs.load() << " allocations in the steady state";
}

int main(int argc, char** argv) {
    SCRATCH = argc > 1 ? argv[1] : "build/scratch";
    std::filesystem::create_directories(SCRATCH);
    return check::run_all();
}
