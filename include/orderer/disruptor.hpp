// disruptor.hpp — the LMAX Disruptor in C++20 (orderer-rust's
// orderer-disruptor, ported; docs/DESIGN.md §2).
//
// Protocol (why the plain slot array is race-free):
//   write: only with an unpublished claim. A claim on s is granted only when
//          every gating consumer has passed s - size.
//   read:  only after observing s published (acquire of the cursor or the
//          slot's availability flag, released by the writer after writing),
//          and only until the reader's own sequence passes s (a release).
// Readers get const T&. Verified by tests/ring_test.cpp under TSan in CI.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace orderer::disruptor {

inline constexpr std::int64_t INITIAL = -1;

inline void cpu_relax() {
#if defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#elif defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#endif
}

/// A cache-line-isolated counter. 128 B: Apple Silicon's line, and two x86
/// lines (adjacent-line prefetch).
struct alignas(128) Sequence {
    std::atomic<std::int64_t> v{INITIAL};
    Sequence() = default;
    explicit Sequence(std::int64_t x) : v(x) {}
    std::int64_t get() const { return v.load(std::memory_order_acquire); }
    void set(std::int64_t x) { v.store(x, std::memory_order_release); }
};

inline std::int64_t min_of(const std::vector<std::shared_ptr<Sequence>>& seqs, std::int64_t floor) {
    std::int64_t m = floor;
    for (const auto& s : seqs) m = std::min(m, s->get());
    return m;
}

// ---- wait strategies -----------------------------------------------------------

enum class WaitKind : std::uint8_t { BusySpin, Yield, Backoff, Blocking };

struct WaitStrategy {
    WaitKind kind = WaitKind::Backoff;
    std::uint32_t spin = 256, yields = 64;
    std::chrono::nanoseconds park_min{std::chrono::microseconds(20)};
    std::chrono::nanoseconds park_max{std::chrono::milliseconds(1)};

    static WaitStrategy busy_spin() { return {WaitKind::BusySpin}; }
    static WaitStrategy yield() { return {WaitKind::Yield}; }
    static WaitStrategy backoff() { return {WaitKind::Backoff}; }
    static WaitStrategy blocking() { return {WaitKind::Blocking}; }
};

/// Wakes Blocking waiters; one relaxed load when nobody is blocked.
struct Notifier {
    std::atomic<std::size_t> waiters{0};
    std::mutex m;
    std::condition_variable cv;

    void signal() {
        if (waiters.load(std::memory_order_relaxed) != 0) wake_all();
    }
    void wake_all() {
        std::lock_guard<std::mutex> g(m);
        cv.notify_all();
    }
    void block() {
        waiters.fetch_add(1, std::memory_order_seq_cst);
        {
            std::unique_lock<std::mutex> g(m);
            cv.wait_for(g, std::chrono::milliseconds(1));
        }
        waiters.fetch_sub(1, std::memory_order_seq_cst);
    }
};

class Waiter {
  public:
    explicit Waiter(WaitStrategy s = {}) : s_(s) {}
    void set(WaitStrategy s) { *this = Waiter(s); }
    void reset() { step_ = 0; }
    void idle(Notifier& n) {
        switch (s_.kind) {
            case WaitKind::BusySpin: cpu_relax(); break;
            case WaitKind::Yield:
                if (step_ < 100) { ++step_; cpu_relax(); }
                else std::this_thread::yield();
                break;
            case WaitKind::Backoff:
                if (step_ < s_.spin) { ++step_; cpu_relax(); }
                else if (step_ < s_.spin + s_.yields) { ++step_; std::this_thread::yield(); }
                else {
                    if (step_ == s_.spin + s_.yields) { ++step_; park_ = s_.park_min; }
                    std::this_thread::sleep_for(park_);
                    park_ = std::min(park_ * 2, s_.park_max);
                }
                break;
            case WaitKind::Blocking:
                if (step_ < 100) { ++step_; cpu_relax(); }
                else n.block();
                break;
        }
    }

  private:
    WaitStrategy s_;
    std::uint32_t step_ = 0;
    std::chrono::nanoseconds park_{0};
};

// ---- shared ring state ---------------------------------------------------------

enum class ProducerKind : std::uint8_t { Single, Multi };
enum class Publish : std::uint8_t { Ok, Full, Alerted };

template <class T>
struct Shared {
    std::unique_ptr<T[]> slots;
    std::int64_t size, mask;
    int shift;
    ProducerKind kind;
    Sequence cursor;  // Single: published. Multi: claimed.
    std::unique_ptr<std::atomic<std::int32_t>[]> available;  // Multi: lap per slot
    Sequence gating_cache;
    std::vector<std::shared_ptr<Sequence>> gating;
    Notifier notifier;
    std::atomic<bool> alerted{false};

    Shared(std::size_t n, ProducerKind k, std::vector<std::shared_ptr<Sequence>> g)
        : slots(new T[n]()), size(std::int64_t(n)), mask(std::int64_t(n) - 1),
          shift(__builtin_ctzll(n)), kind(k), gating(std::move(g)) {
        if (k == ProducerKind::Multi) {
            available.reset(new std::atomic<std::int32_t>[n]);
            for (std::size_t i = 0; i < n; ++i) available[i].store(-1, std::memory_order_relaxed);
        }
    }

    T& slot(std::int64_t s) { return slots[std::size_t(s & mask)]; }
    std::int64_t min_gating() const { return min_of(gating, INT64_MAX); }
    std::int32_t lap(std::int64_t s) const { return std::int32_t(s >> shift); }
    void set_available(std::int64_t s) {
        available[std::size_t(s & mask)].store(lap(s), std::memory_order_release);
    }
    bool is_available(std::int64_t s) const {
        return available[std::size_t(s & mask)].load(std::memory_order_acquire) == lap(s);
    }
    /// Highest s' ≤ limit with every seq in [lo, s'] published (lo-1 if none).
    std::int64_t published_upto(std::int64_t lo, std::int64_t limit) const {
        std::int64_t hi = std::min(cursor.get(), limit);
        if (kind == ProducerKind::Single) return hi;
        for (std::int64_t s = lo; s <= hi; ++s)
            if (!is_available(s)) return s - 1;
        return hi;
    }
    std::int64_t published_high_water() const {
        if (kind == ProducerKind::Single) return cursor.get();
        std::int64_t floor = std::min(min_gating(), cursor.get());
        return published_upto(floor + 1, INT64_MAX);
    }
    bool is_alerted() const { return alerted.load(std::memory_order_acquire); }
    void alert() {
        alerted.store(true, std::memory_order_release);
        notifier.wake_all();
    }
};

inline void producer_backoff(std::uint32_t& step) {
    if (step < 64) { ++step; cpu_relax(); }
    else std::this_thread::yield();
}

template <class T>
class RingControl {
  public:
    explicit RingControl(std::shared_ptr<Shared<T>> s) : s_(std::move(s)) {}
    void alert() { s_->alert(); }
    bool is_alerted() const { return s_->is_alerted(); }
    std::int64_t published() const { return s_->published_high_water(); }
    std::int64_t consumed() const { return s_->min_gating(); }

  private:
    std::shared_ptr<Shared<T>> s_;
};

/// The only producer of a Single ring. stage() + commit() publish a whole
/// batch with one release store.
template <class T>
class SingleProducer {
  public:
    explicit SingleProducer(std::shared_ptr<Shared<T>> s)
        : s_(std::move(s)), next_(s_->cursor.get()), published_(next_), cached_gate_(next_) {}
    SingleProducer(SingleProducer&&) noexcept = default;
    SingleProducer& operator=(SingleProducer&&) noexcept = default;
    ~SingleProducer() {
        if (s_) commit();
    }

    std::size_t size() const { return std::size_t(s_->size); }
    std::size_t staged() const { return std::size_t(next_ - published_); }

    template <class F>
    Publish stage(F&& f) {
        if (wait_room(1) != Publish::Ok) return Publish::Alerted;
        ++next_;
        f(s_->slot(next_));
        return Publish::Ok;
    }
    void commit() {
        if (next_ != published_) {
            published_ = next_;
            s_->cursor.set(next_);
            s_->notifier.signal();
        }
    }
    template <class F>
    Publish publish(F&& f) {
        Publish r = stage(std::forward<F>(f));
        commit();
        return r;
    }
    template <class F>
    Publish try_publish(F&& f) {
        if (!has_room(1)) return Publish::Full;
        return publish(std::forward<F>(f));
    }
    template <class F>
    Publish publish_batch(std::size_t n, F&& f) {
        if (wait_room(std::int64_t(n)) != Publish::Ok) return Publish::Alerted;
        for (std::size_t i = 0; i < n; ++i) { ++next_; f(i, s_->slot(next_)); }
        commit();
        return Publish::Ok;
    }
    RingControl<T> control() const { return RingControl<T>(s_); }

  private:
    bool has_room(std::int64_t n) {
        std::int64_t wrap = next_ + n - s_->size;
        if (wrap <= cached_gate_) return true;
        cached_gate_ = s_->min_gating();
        return wrap <= cached_gate_;
    }
    Publish wait_room(std::int64_t n) {
        if (has_room(n)) return Publish::Ok;
        commit();  // consumers can't free space they can't see
        std::uint32_t step = 0;
        while (!has_room(n)) {
            if (s_->is_alerted()) return Publish::Alerted;
            producer_backoff(step);
        }
        return Publish::Ok;
    }

    std::shared_ptr<Shared<T>> s_;
    std::int64_t next_, published_, cached_gate_;
};

/// A producer of a Multi ring; copies publish concurrently.
template <class T>
class MultiProducer {
  public:
    explicit MultiProducer(std::shared_ptr<Shared<T>> s) : s_(std::move(s)) {}
    std::size_t size() const { return std::size_t(s_->size); }

    template <class F>
    Publish publish_batch(std::size_t n, F&& f) {
        std::int64_t hi = s_->cursor.v.fetch_add(std::int64_t(n), std::memory_order_acq_rel) +
                          std::int64_t(n);
        std::uint32_t step = 0;
        while (!room_for(hi)) {
            if (s_->is_alerted()) return Publish::Alerted;
            producer_backoff(step);
        }
        fill(hi - std::int64_t(n) + 1, hi, f);
        return Publish::Ok;
    }
    template <class F>
    Publish publish(F&& f) {
        return publish_batch(1, [&](std::size_t, T& t) { f(t); });
    }
    /// Claim by CAS only if it fits: a fetch_add claim could not be backed out.
    template <class F>
    Publish try_publish_batch(std::size_t n, F&& f) {
        for (;;) {
            if (s_->is_alerted()) return Publish::Alerted;
            std::int64_t cur = s_->cursor.get();
            std::int64_t hi = cur + std::int64_t(n);
            if (!room_for(hi)) return Publish::Full;
            if (s_->cursor.v.compare_exchange_weak(cur, hi, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
                fill(cur + 1, hi, f);
                return Publish::Ok;
            }
        }
    }
    template <class F>
    Publish try_publish(F&& f) {
        return try_publish_batch(1, [&](std::size_t, T& t) { f(t); });
    }
    RingControl<T> control() const { return RingControl<T>(s_); }

  private:
    bool room_for(std::int64_t hi) {
        std::int64_t wrap = hi - s_->size;
        if (wrap <= s_->gating_cache.get()) return true;
        std::int64_t g = s_->min_gating();
        s_->gating_cache.set(g);
        return wrap <= g;
    }
    template <class F>
    void fill(std::int64_t lo, std::int64_t hi, F& f) {
        for (std::int64_t s = lo; s <= hi; ++s) f(std::size_t(s - lo), s_->slot(s));
        for (std::int64_t s = lo; s <= hi; ++s) s_->set_available(s);
        s_->notifier.signal();
    }

    std::shared_ptr<Shared<T>> s_;
};

/// One consumer: a sequence barrier (cursor + upstream consumers) plus its
/// own "processed up to" watermark.
template <class T>
class Consumer {
  public:
    Consumer(std::shared_ptr<Shared<T>> s, std::vector<std::shared_ptr<Sequence>> deps,
             std::shared_ptr<Sequence> seq, WaitStrategy w)
        : s_(std::move(s)), deps_(std::move(deps)), seq_(std::move(seq)), next_(seq_->get() + 1),
          waiter_(w) {}

    std::shared_ptr<Sequence> sequence() const { return seq_; }
    void set_wait(WaitStrategy w) { waiter_.set(w); }
    void set_max_batch(std::size_t n) { max_batch_ = std::int64_t(n); }
    std::int64_t next_seq() const { return next_; }
    bool is_alerted() const { return s_->is_alerted(); }

    std::int64_t available() const {
        std::int64_t limit = next_ + max_batch_ - 1;
        if (deps_.empty()) return s_->published_upto(next_, limit);
        return min_of(deps_, limit);
    }

    /// Process everything available (up to the batch cap) without waiting:
    /// f(const T&, seq, end_of_batch). Returns the count.
    template <class F>
    std::size_t poll(F&& f) {
        std::int64_t avail = available();
        if (avail < next_) return 0;
        for (std::int64_t s = next_; s <= avail; ++s) f(std::as_const(s_->slot(s)), s, s == avail);
        std::size_t n = std::size_t(avail - next_ + 1);
        next_ = avail + 1;
        seq_->set(avail);
        s_->notifier.signal();
        return n;
    }
    /// Wait for at least one event, then poll. false once alerted and empty.
    template <class F>
    bool wait_poll(F&& f) {
        for (;;) {
            if (poll(f) > 0) { waiter_.reset(); return true; }
            if (s_->is_alerted()) return false;
            waiter_.idle(s_->notifier);
        }
    }
    void idle() { waiter_.idle(s_->notifier); }
    void reset_idle() { waiter_.reset(); }

  private:
    std::shared_ptr<Shared<T>> s_;
    std::vector<std::shared_ptr<Sequence>> deps_;
    std::shared_ptr<Sequence> seq_;
    std::int64_t next_;
    std::int64_t max_batch_ = 1024;
    Waiter waiter_;
};

/// Declare consumers (and their upstream dependencies), then build the
/// producer and every consumer at once. The producer gates on terminal
/// consumers.
template <class T>
class RingBuilder {
  public:
    explicit RingBuilder(std::size_t size) : size_(size) {
        if (size == 0 || (size & (size - 1)) != 0) throw std::invalid_argument("ring size must be a power of two");
    }
    std::size_t consumer(std::vector<std::size_t> deps = {}, WaitStrategy w = {}) {
        for (auto d : deps)
            if (d >= specs_.size()) throw std::invalid_argument("dependencies must be declared first");
        specs_.push_back({std::move(deps), w});
        return specs_.size() - 1;
    }
    std::pair<SingleProducer<T>, std::vector<Consumer<T>>> build_single() {
        auto [shared, cons] = build(ProducerKind::Single);
        return {SingleProducer<T>(shared), std::move(cons)};
    }
    std::pair<MultiProducer<T>, std::vector<Consumer<T>>> build_multi() {
        auto [shared, cons] = build(ProducerKind::Multi);
        return {MultiProducer<T>(shared), std::move(cons)};
    }

  private:
    struct Spec {
        std::vector<std::size_t> deps;
        WaitStrategy w;
    };
    std::pair<std::shared_ptr<Shared<T>>, std::vector<Consumer<T>>> build(ProducerKind k) {
        std::vector<std::shared_ptr<Sequence>> seqs;
        for (std::size_t i = 0; i < specs_.size(); ++i) seqs.push_back(std::make_shared<Sequence>());
        std::vector<bool> depended(specs_.size(), false);
        for (auto& sp : specs_)
            for (auto d : sp.deps) depended[d] = true;
        std::vector<std::shared_ptr<Sequence>> gating;
        for (std::size_t i = 0; i < seqs.size(); ++i)
            if (!depended[i]) gating.push_back(seqs[i]);
        auto shared = std::make_shared<Shared<T>>(size_, k, std::move(gating));
        std::vector<Consumer<T>> cons;
        for (std::size_t i = 0; i < specs_.size(); ++i) {
            std::vector<std::shared_ptr<Sequence>> deps;
            for (auto d : specs_[i].deps) deps.push_back(seqs[d]);
            cons.emplace_back(shared, std::move(deps), seqs[i], specs_[i].w);
        }
        return {shared, std::move(cons)};
    }

    std::size_t size_;
    std::vector<Spec> specs_;
};

}  // namespace orderer::disruptor
