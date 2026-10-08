// egress.hpp — ring slot types and pluggable per-partition event consumers
// (spec/PIPELINE.md §7).
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <matcher/matcher.hpp>

namespace orderer {

using namespace matcher;

/// Control operations ride the rings so they cut every partition at the same
/// point of the ingress order (spec/PIPELINE.md §6).
enum class Control : std::uint8_t { None = 0, Barrier, Snapshot, Shutdown };

/// Ingress / inbox slot. Implementation-private layout.
struct CmdMsg {
    std::uint64_t iseq = 0;   // commands: ingress seq; controls: the cut
    std::uint64_t t_pub = 0;  // publish ns since the pipeline epoch (0 = untimed)
    std::uint64_t arg = 0;    // barrier epoch / snapshot op id
    Symbol symbol = 0;
    Control ctl = Control::None;
    Command cmd{};
};

/// Outbox slot: one event (or a control passing through to egress).
struct EvtMsg {
    std::uint64_t iseq = 0;  // the causing command
    std::uint64_t seq = 0;   // per-book event sequence
    std::uint64_t t_pub = 0;
    std::uint64_t arg = 0;
    Symbol symbol = 0;
    Control ctl = Control::None;
    Event ev{};
};

/// What a partition's egress instances know.
struct EgressCtx {
    std::uint32_t partition = 0, partitions = 1;
    std::chrono::steady_clock::time_point epoch;
    std::shared_ptr<std::atomic<std::uint64_t>> durable;
    /// Highest iseq covered by a completed fsync (UINT64_MAX without journals).
    std::uint64_t durable_iseq() const { return durable->load(std::memory_order_acquire); }
    std::uint64_t now_ns() const {
        return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now() - epoch)
                                 .count());
    }
};

/// One partition's consumer of events.
class Egress {
  public:
    virtual ~Egress() = default;
    virtual void on_event(const EvtMsg& m) = 0;  // every event, partition order
    virtual void on_batch_end() {}                // after a ring batch / before a drain completes
    virtual void on_idle() {}                     // while idle: release gated work
    virtual void on_shutdown() {}                 // once, after every event
};

/// Creates one Egress per partition.
using EgressFactory = std::function<std::unique_ptr<Egress>(const EgressCtx&)>;

// ---- Collect: canonical lines per partition (harnesses, tests) ---------------------

/// What a Collect gathered, per partition.
class CollectHandle {
  public:
    std::vector<std::string> take() {
        std::lock_guard<std::mutex> g(m_);
        std::vector<std::string> out;
        for (auto& b : bufs_) out.push_back(std::move(b)), b.clear();
        return out;
    }
    /// spec/HARNESS.md §3 listing: partition 0's lines, then 1's, …
    std::string listing() {
        std::lock_guard<std::mutex> g(m_);
        std::string out;
        for (auto& b : bufs_) out += b;
        return out;
    }

  private:
    friend struct CollectEgress;
    friend inline std::pair<EgressFactory, std::shared_ptr<CollectHandle>> collect(bool);
    std::mutex m_;
    std::vector<std::string> bufs_;
};

struct CollectEgress final : Egress {
    std::uint32_t partition;
    bool tagged;
    std::string local, scratch;
    std::shared_ptr<CollectHandle> h;
    void on_event(const EvtMsg& m) override {
        if (tagged) write_canonical_sym(m.seq, m.symbol, m.ev, local);
        else write_canonical(m.seq, m.ev, local);
        local += '\n';
    }
    void on_batch_end() override {
        if (local.empty()) return;
        std::lock_guard<std::mutex> g(h->m_);
        h->bufs_[partition] += local;
        local.clear();
    }
    void on_shutdown() override { on_batch_end(); }
};

/// Canonical lines per partition, symbol-tagged or not.
inline std::pair<EgressFactory, std::shared_ptr<CollectHandle>> collect(bool tagged) {
    auto h = std::make_shared<CollectHandle>();
    EgressFactory f = [h, tagged](const EgressCtx& ctx) -> std::unique_ptr<Egress> {
        {
            std::lock_guard<std::mutex> g(h->m_);
            if (h->bufs_.size() < ctx.partitions) h->bufs_.resize(ctx.partitions);
        }
        auto e = std::make_unique<CollectEgress>();
        e->partition = ctx.partition;
        e->tagged = tagged;
        e->h = h;
        e->local.reserve(1 << 16);
        return e;
    };
    return {f, h};
}

// ---- Callback --------------------------------------------------------------------------

/// f(partition, msg) for every event; f is copied per partition.
template <class F>
EgressFactory callback(F f) {
    struct CallbackEgress final : Egress {
        std::uint32_t p;
        F f;
        CallbackEgress(std::uint32_t p, F f) : p(p), f(std::move(f)) {}
        void on_event(const EvtMsg& m) override { f(p, m); }
    };
    return [f](const EgressCtx& ctx) -> std::unique_ptr<Egress> {
        return std::make_unique<CallbackEgress>(ctx.partition, f);
    };
}

// ---- Acks: durability-gated delivery ---------------------------------------------------

/// f(partition, msg) only once the causing command is durable
/// (spec/PIPELINE.md §5). Pending events wait in a queue.
template <class F>
EgressFactory acks(F f) {
    struct AckEgress final : Egress {
        EgressCtx ctx;
        F f;
        std::deque<EvtMsg> pending;
        AckEgress(EgressCtx c, F f) : ctx(std::move(c)), f(std::move(f)) {}
        void release() {
            if (pending.empty()) return;
            std::uint64_t d = ctx.durable_iseq();
            while (!pending.empty() && pending.front().iseq <= d) {
                f(ctx.partition, pending.front());
                pending.pop_front();
            }
        }
        void on_event(const EvtMsg& m) override { pending.push_back(m); }
        void on_batch_end() override { release(); }
        void on_idle() override { release(); }
        void on_shutdown() override { release(); }
    };
    return [f](const EgressCtx& ctx) -> std::unique_ptr<Egress> {
        return std::make_unique<AckEgress>(ctx, f);
    };
}

// ---- Metrics: counts + end-to-end latency ----------------------------------------------

struct PartitionMetrics {
    std::uint32_t partition = 0;
    std::uint64_t events = 0, commands = 0, trades = 0;
    std::vector<std::uint64_t> latencies;  // ns, arrival order
};

class MetricsHandle {
  public:
    std::vector<PartitionMetrics> results() {
        std::lock_guard<std::mutex> g(m_);
        return out_;
    }

  private:
    friend inline std::pair<EgressFactory, std::shared_ptr<MetricsHandle>> metrics(std::size_t);
    std::mutex m_;
    std::vector<PartitionMetrics> out_;
};

/// One latency sample per command, at its first event: now - t_pub
/// (spec/BENCH.md §2.2 step 5). Samples preallocated per partition.
inline std::pair<EgressFactory, std::shared_ptr<MetricsHandle>> metrics(std::size_t sample_capacity) {
    struct MetricsEgress final : Egress {
        EgressCtx ctx;
        PartitionMetrics m;
        std::uint64_t last_iseq = 0;
        std::shared_ptr<MetricsHandle> h;
        void on_event(const EvtMsg& e) override {
            ++m.events;
            if (e.ev.kind == Event::Kind::Trade) ++m.trades;
            if (e.iseq != last_iseq) {
                last_iseq = e.iseq;
                ++m.commands;
                if (e.t_pub) {
                    std::uint64_t now = ctx.now_ns();
                    m.latencies.push_back(now > e.t_pub ? now - e.t_pub : 0);
                }
            }
        }
        void on_shutdown() override {
            std::lock_guard<std::mutex> g(h->m_);
            h->out_.push_back(std::move(m));
        }
    };
    auto h = std::make_shared<MetricsHandle>();
    EgressFactory f = [h, sample_capacity](const EgressCtx& ctx) -> std::unique_ptr<Egress> {
        auto e = std::make_unique<MetricsEgress>();
        e->ctx = ctx;
        e->m.partition = ctx.partition;
        e->m.latencies.reserve(sample_capacity);
        e->h = h;
        return e;
    };
    return {f, h};
}

}  // namespace orderer
