// pipeline.hpp — rings, threads and control (spec/PIPELINE.md).
//
//   Handle::publish ─▶ ingress (multi-producer) ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress
//                                                          └─▶ …            (journal + apply)
//
// router (1): sole ingress consumer; stamps iseq, routes, broadcasts controls,
//   commits every inbox once per batch.
// engine[p]: encodes each command's journal record into its partition's
//   ChunkWriter, then applies it (journal-before-apply, in-thread), staging
//   events into outbox[p].
// egress (grouped): runs the plugs, marks drain epochs.
// I/O threads (one per journal file, inside ChunkWriter): write + fsync.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core.hpp"
#include "disruptor.hpp"
#include "egress.hpp"
#include "journal.hpp"
#include "routing.hpp"

namespace orderer {

using disruptor::WaitStrategy;

struct Error : std::runtime_error {
    enum class Kind : std::uint8_t { Closed, Full, Failed, Config, Io } kind;
    Error(Kind k, const std::string& m) : std::runtime_error(m), kind(k) {}
};

/// Publish outcome: Ok means sequenced and will be applied — not durable.
enum class Status : std::uint8_t { Ok, Closed, Full };

/// Wait strategy per stage (not observable).
struct Waits {
    WaitStrategy router = WaitStrategy::backoff(), engine = WaitStrategy::backoff(),
                 egress = WaitStrategy::backoff();
    static Waits relaxed() { return {}; }
    /// Router and engines busy-spin; egress backs off. The bench configuration.
    static Waits low_latency() {
        return {WaitStrategy::busy_spin(), WaitStrategy::busy_spin(), WaitStrategy::backoff()};
    }
};

/// Starting state after recovery.
template <class C>
struct Initial {
    std::vector<C> cores;
    std::uint64_t next_iseq = 1;
};

/// A merged matcher-snap/1 snapshot plus its cut (spec/JOURNAL.md §4).
struct Snapshot {
    std::string body;
    std::uint64_t iseq = 0;
    std::uint32_t partitions = 1;
    std::string meta() const {
        return "{\"format\":\"orderer-meta/1\",\"iseq\":" + std::to_string(iseq) +
               ",\"partitions\":" + std::to_string(partitions) + "}\n";
    }
    void write(const fs::path& path) const;
};

inline fs::path meta_path(const fs::path& p) { return fs::path(p.string() + ".meta"); }

inline void Snapshot::write(const fs::path& path) const {
    std::ofstream(path, std::ios::binary) << body;
    std::ofstream(meta_path(path), std::ios::binary) << meta();
}

namespace detail {

struct alignas(128) Padded {
    std::atomic<std::uint64_t> v{0};
};
struct alignas(128) InFlight {
    std::atomic<bool> v{false};
};

struct SnapState {
    std::vector<Block> blocks;
    std::uint32_t remaining = 0;
    std::uint64_t cut = 0;
};

struct Shared {
    std::uint32_t partitions;
    BookConfig book;
    std::chrono::steady_clock::time_point epoch = std::chrono::steady_clock::now();
    std::atomic<bool> timestamps{false}, closed{false}, failed{false};
    std::mutex fail_m;
    std::string failure;
    std::mutex handles_m;
    std::vector<std::shared_ptr<InFlight>> handles;
    std::atomic<std::uint64_t> next_epoch{0}, next_op{0};
    std::unique_ptr<Padded[]> egress_epoch;
    std::mutex snaps_m;
    std::condition_variable snaps_cv;
    std::map<std::uint64_t, SnapState> snaps;
    std::vector<std::shared_ptr<std::atomic<std::uint64_t>>> flushed, durable;
    std::vector<std::shared_ptr<IoStats>> io;
    std::vector<std::shared_ptr<EngineCounters>> counters;
    std::optional<JournalConfig> journal;
    std::mutex alerts_m;  // threads may fail while later rings are still registering
    std::vector<std::function<void()>> alerts;

    void add_alert(std::function<void()> a) {
        std::lock_guard<std::mutex> g(alerts_m);
        alerts.push_back(std::move(a));
    }
    void alert_all() {
        std::lock_guard<std::mutex> g(alerts_m);
        for (auto& a : alerts) a();
    }
    void fail(const std::string& m) {
        {
            std::lock_guard<std::mutex> g(fail_m);
            if (failure.empty()) failure = m;
        }
        failed.store(true, std::memory_order_seq_cst);
        alert_all();
        snaps_cv.notify_all();
    }
    void check() {
        if (failed.load(std::memory_order_acquire)) {
            std::lock_guard<std::mutex> g(fail_m);
            throw Error(Error::Kind::Failed, "pipeline failed: " + failure);
        }
    }
    std::uint64_t now_ns() const {
        if (!timestamps.load(std::memory_order_relaxed)) return 0;
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - epoch).count();
        return std::max<std::uint64_t>(std::uint64_t(ns), 1);
    }
};

/// spin → yield → short sleeps until done(), failing fast.
template <class F>
void wait_until(Shared& sh, F done) {
    std::uint32_t step = 0;
    while (!done()) {
        sh.check();
        ++step;
        if (step < 64) disruptor::cpu_relax();
        else if (step < 256) std::this_thread::yield();
        else std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

}  // namespace detail

/// Publishes commands; copyable, thread-safe. Each copy registers an
/// in-flight flag so shutdown can wait out publishes that began before it
/// closed the pipeline: a publish that returns Ok is always applied.
class Handle {
  public:
    Handle(disruptor::MultiProducer<CmdMsg> ingress, std::shared_ptr<detail::Shared> sh)
        : ingress_(std::move(ingress)), sh_(std::move(sh)), flag_(std::make_shared<detail::InFlight>()) {
        std::lock_guard<std::mutex> g(sh_->handles_m);
        sh_->handles.push_back(flag_);
    }
    Handle(const Handle& o) : Handle(o.ingress_, o.sh_) {}
    Handle& operator=(const Handle&) = delete;
    ~Handle() {
        std::lock_guard<std::mutex> g(sh_->handles_m);
        auto& v = sh_->handles;
        v.erase(std::remove(v.begin(), v.end(), flag_), v.end());
    }

    /// Sequence one command (blocks while ingress is full).
    Status publish(Symbol sym, const Command& cmd) {
        if (!enter()) return Status::Closed;
        std::uint64_t t = sh_->now_ns();
        auto r = ingress_.publish([&](CmdMsg& m) { fill(m, sym, cmd, t); });
        exit();
        return r == disruptor::Publish::Ok ? Status::Ok : Status::Closed;
    }
    /// Sequence one command, or Full without waiting.
    Status try_publish(Symbol sym, const Command& cmd) {
        if (!enter()) return Status::Closed;
        std::uint64_t t = sh_->now_ns();
        auto r = ingress_.try_publish([&](CmdMsg& m) { fill(m, sym, cmd, t); });
        exit();
        if (r == disruptor::Publish::Ok) return Status::Ok;
        return r == disruptor::Publish::Full ? Status::Full : Status::Closed;
    }
    /// Many commands, one claim per chunk (consecutive iseqs within a chunk).
    Status publish_batch(const std::pair<Symbol, Command>* cmds, std::size_t n) {
        if (!enter()) return Status::Closed;
        std::size_t chunk = std::min<std::size_t>(ingress_.size(), 256);
        auto r = disruptor::Publish::Ok;
        for (std::size_t off = 0; off < n && r == disruptor::Publish::Ok; off += chunk) {
            std::size_t k = std::min(chunk, n - off);
            std::uint64_t t = sh_->now_ns();
            r = ingress_.publish_batch(k, [&](std::size_t i, CmdMsg& m) {
                fill(m, cmds[off + i].first, cmds[off + i].second, t);
            });
        }
        exit();
        return r == disruptor::Publish::Ok ? Status::Ok : Status::Closed;
    }
    Status publish_batch(const std::vector<std::pair<Symbol, Command>>& v) { return publish_batch(v.data(), v.size()); }

  private:
    template <class>
    friend class Pipeline;
    bool enter() {
        flag_->v.store(true, std::memory_order_seq_cst);
        if (sh_->closed.load(std::memory_order_seq_cst)) {
            flag_->v.store(false, std::memory_order_release);
            return false;
        }
        return true;
    }
    void exit() { flag_->v.store(false, std::memory_order_release); }
    static void fill(CmdMsg& m, Symbol sym, const Command& cmd, std::uint64_t t) {
        m.symbol = sym;
        m.t_pub = t;
        m.ctl = Control::None;
        m.cmd = cmd;
    }

    disruptor::MultiProducer<CmdMsg> ingress_;
    std::shared_ptr<detail::Shared> sh_;
    std::shared_ptr<detail::InFlight> flag_;
};

template <class C>
class Pipeline;

template <class C>
class PipelineBuilder {
  public:
    PipelineBuilder& book_config(BookConfig c) { book_ = c; return *this; }
    PipelineBuilder& partitions(std::uint32_t p) { partitions_ = p; map_.reset(); return *this; }
    PipelineBuilder& partition_map(PartitionMap m) { partitions_ = m.partitions(); map_ = std::move(m); return *this; }
    PipelineBuilder& ring_sizes(std::size_t ingress, std::size_t inbox, std::size_t outbox) {
        ingress_ = ingress; inbox_ = inbox; outbox_ = outbox; return *this;
    }
    PipelineBuilder& waits(Waits w) { waits_ = w; return *this; }
    PipelineBuilder& journal(JournalConfig j) { journal_ = std::move(j); return *this; }
    PipelineBuilder& egress(EgressFactory f) { egress_.push_back(std::move(f)); return *this; }
    PipelineBuilder& timestamps(bool on) { timestamps_ = on; return *this; }
    /// Threads running the partitions' egress plugs (partition p → p % n).
    PipelineBuilder& egress_threads(std::size_t n) { egress_threads_ = std::max<std::size_t>(n, 1); return *this; }
    PipelineBuilder& initial(Initial<C> i) { initial_ = std::move(i); return *this; }
    /// Take a checkpoint (spec/JOURNAL.md §6) every `interval` from a
    /// background thread. Needs journals; shutdown stops the thread first; a
    /// failed checkpoint fails the pipeline.
    PipelineBuilder& checkpoint_every(std::chrono::nanoseconds interval) { checkpoint_every_ = interval; return *this; }
    std::unique_ptr<Pipeline<C>> build() { return std::unique_ptr<Pipeline<C>>(new Pipeline<C>(*this)); }

  private:
    friend class Pipeline<C>;
    BookConfig book_{};
    std::uint32_t partitions_ = 1;
    std::optional<PartitionMap> map_;
    // tuned in orderer-rust phase 6: L2-friendly, ~4x lower queueing latency
    std::size_t ingress_ = 1 << 14, inbox_ = 1 << 12, outbox_ = 1 << 13;
    Waits waits_{};
    std::optional<JournalConfig> journal_;
    std::vector<EgressFactory> egress_;
    bool timestamps_ = false;
    std::size_t egress_threads_ = 1;
    std::optional<Initial<C>> initial_;
    std::optional<std::chrono::nanoseconds> checkpoint_every_;
};

/// A running pipeline. Destroying it shuts it down.
template <class C>
class Pipeline {
  public:
    static PipelineBuilder<C> builder() { return {}; }

    Pipeline(const Pipeline&) = delete;
    ~Pipeline() {
        try { shutdown(); } catch (...) {}
    }

    Handle handle() const { return *handle_; }
    Status publish(Symbol s, const Command& c) { return handle_->publish(s, c); }
    Status try_publish(Symbol s, const Command& c) { return handle_->try_publish(s, c); }
    Status publish_batch(const std::vector<std::pair<Symbol, Command>>& v) { return handle_->publish_batch(v); }
    Status publish_batch(const std::pair<Symbol, Command>* cmds, std::size_t n) { return handle_->publish_batch(cmds, n); }
    std::uint32_t partitions() const { return sh_->partitions; }
    std::uint32_t partition_of(Symbol s) const { return map_.partition(s); }
    BookConfig book_config() const { return sh_->book; }
    void set_timestamps(bool on) { sh_->timestamps.store(on, std::memory_order_relaxed); }
    std::uint64_t durable_iseq(std::uint32_t p) const { return sh_->durable[p]->load(std::memory_order_acquire); }

    /// Barrier: returns once every command published before the call has been
    /// applied and delivered to every egress plug.
    void drain() { drain_on(*handle_); }

  private:
    void drain_on(Handle& h) {
        std::uint64_t epoch = sh_->next_epoch.fetch_add(1) + 1;
        publish_ctl(Control::Barrier, epoch, h);
        detail::wait_until(*sh_, [&] {
            for (std::uint32_t p = 0; p < sh_->partitions; ++p)
                if (sh_->egress_epoch[p].v.load(std::memory_order_acquire) < epoch) return false;
            return true;
        });
    }

    /// A consistent snapshot of every book, cut at this point of the ingress order.
  public:
    Snapshot snapshot() { return snapshot_op(Control::Snapshot, *handle_); }

    /// A checkpoint (spec/JOURNAL.md §6): a snapshot cut here; every journal
    /// rotates onto a new segment at the cut; the snapshot is written durably
    /// into the journal directory; older segments and checkpoints go.
    Snapshot checkpoint() { return checkpoint_on(*handle_); }

    /// Operational statistics (stats.hpp).
    PipelineStats stats() const {
        auto depth = [](std::int64_t pub, std::int64_t con) { return pub > con ? std::uint64_t(pub - con) : 0; };
        PipelineStats st;
        if (ingress_ctl_) st.ingress_depth = depth(ingress_ctl_->published(), ingress_ctl_->consumed());
        for (std::uint32_t p = 0; p < sh_->partitions; ++p) {
            PartitionStats ps;
            ps.partition = p;
            ps.inbox_depth = depth(inbox_ctl_[p].published(), inbox_ctl_[p].consumed());
            ps.outbox_depth = depth(outbox_ctl_[p].published(), outbox_ctl_[p].consumed());
            ps.commands = sh_->counters[p]->commands.load(std::memory_order_relaxed);
            ps.events = sh_->counters[p]->events.load(std::memory_order_relaxed);
            ps.flushed_iseq = sh_->journal ? sh_->flushed[p]->load(std::memory_order_acquire) : UINT64_MAX;
            ps.durable_iseq = sh_->durable[p]->load(std::memory_order_acquire);
            ps.fsyncs = sh_->io[p]->fsyncs.load(std::memory_order_relaxed);
            ps.fsync_ns_total = sh_->io[p]->fsync_ns_total.load(std::memory_order_relaxed);
            ps.fsync_ns_max = sh_->io[p]->fsync_ns_max.load(std::memory_order_relaxed);
            st.partitions.push_back(ps);
        }
        return st;
    }

  private:
    Snapshot checkpoint_on(Handle& h) {
        if (!sh_->journal) throw Error(Error::Kind::Config, "checkpoint needs journals");
        const auto& cfg = *sh_->journal;
        Snapshot s = snapshot_op(Control::Checkpoint, h);
        drain_on(h);  // every egress has rotated its event journal
        try {
            auto path = checkpoint_path(cfg.dir, s.iseq);
            write_durably(path, s.body);
            write_durably(meta_of(path), s.meta());
            remove_segments_below(cfg.dir, cfg.format, s.iseq);
            remove_checkpoints_below(cfg.dir, s.iseq);
        } catch (const std::exception& e) {
            throw Error(Error::Kind::Io, e.what());
        }
        return s;
    }

    Snapshot snapshot_op(Control ctl, Handle& h) {
        std::uint64_t op = sh_->next_op.fetch_add(1) + 1;
        {
            std::lock_guard<std::mutex> g(sh_->snaps_m);
            sh_->snaps[op].remaining = sh_->partitions;
        }
        publish_ctl(ctl, op, h);
        std::unique_lock<std::mutex> g(sh_->snaps_m);
        for (;;) {
            if (sh_->failed.load()) { g.unlock(); sh_->check(); }
            if (sh_->snaps[op].remaining == 0) break;
            sh_->snaps_cv.wait_for(g, std::chrono::milliseconds(10));
        }
        detail::SnapState st = std::move(sh_->snaps[op]);
        sh_->snaps.erase(op);
        g.unlock();
        std::sort(st.blocks.begin(), st.blocks.end(), [](const Block& a, const Block& b) { return a.first < b.first; });
        Snapshot s;
        write_snapshot_header(sh_->book, s.body);
        for (auto& b : st.blocks) s.body += b.second;
        s.iseq = st.cut;
        s.partitions = sh_->partitions;
        return s;
    }

  public:

    /// Stop accepting commands, drain everything sequenced, stop every
    /// thread. Idempotent. Throws Error(Failed) if any thread failed.
    void shutdown() {
        if (shut_) { sh_->check(); return; }
        shut_ = true;
        if (ckpt_.joinable()) {  // a checkpoint in progress finishes first
            {
                std::lock_guard<std::mutex> g(ckpt_m_);
                ckpt_stop_ = true;
            }
            ckpt_cv_.notify_all();
            ckpt_.join();
        }
        sh_->closed.store(true, std::memory_order_seq_cst);
        std::vector<std::shared_ptr<detail::InFlight>> flags;
        {
            std::lock_guard<std::mutex> g(sh_->handles_m);
            flags = sh_->handles;
        }
        try {
            detail::wait_until(*sh_, [&] {
                for (auto& f : flags)
                    if (f->v.load(std::memory_order_seq_cst)) return false;
                return true;
            });
        } catch (...) {}
        if (!sh_->failed.load()) {
            auto r = handle_->ingress_.publish([](CmdMsg& m) {
                m.symbol = 0; m.t_pub = 0; m.arg = 0; m.ctl = Control::Shutdown;
            });
            if (r != disruptor::Publish::Ok) sh_->fail("ingress alerted before shutdown");
        }
        for (auto& t : threads_) t.join();
        threads_.clear();
        sh_->alert_all();
        sh_->check();
    }

  private:
    friend class PipelineBuilder<C>;

    void publish_ctl(Control c, std::uint64_t arg, Handle& h) {
        sh_->check();
        if (sh_->closed.load(std::memory_order_seq_cst)) throw Error(Error::Kind::Closed, "pipeline closed");
        auto r = h.ingress_.publish([&](CmdMsg& m) {
            m.symbol = 0; m.t_pub = 0; m.arg = arg; m.ctl = c;
        });
        if (r != disruptor::Publish::Ok) throw Error(Error::Kind::Closed, "pipeline closed");
    }

    /// Opens a partition's next journal segment (spec/JOURNAL.md §6 step 2).
    struct Segmenter {
        fs::path dir;
        JournalFormat format{};
        Kind kind{};
        std::uint32_t p = 0, partitions = 1;
        BookConfig book{};
        void rotate(ChunkWriter& w, std::uint64_t cut) const {
            w.rotate(open_segment(dir, format, kind, p, partitions, book, cut));
        }
    };

    struct EgressPart {
        std::uint32_t p;
        disruptor::Consumer<EvtMsg> outbox;
        std::vector<std::unique_ptr<Egress>> plugs;
        EgressCtx ctx;
        std::uint64_t last_iseq = 0;
        bool stopped = false;
    };

    /// The event journal as the first plug of its partition.
    struct EvtJournal final : Egress {
        std::unique_ptr<ChunkWriter> w;
        JournalFormat f;
        Segmenter seg;
        void on_checkpoint(std::uint64_t cut) override { seg.rotate(*w, cut); }
        std::chrono::steady_clock::time_point last_handoff = std::chrono::steady_clock::now();
        void on_event(const EvtMsg& m) override {
            w->reserve(MAX_RECORD);
            push_evt(w->buf(), f, m.seq, m.symbol, m.ev);
            w->record(m.seq);
        }
        void on_idle() override {
            if (w->pending() && std::chrono::steady_clock::now() - last_handoff >= std::chrono::microseconds(50)) {
                w->hand_off();
                last_handoff = std::chrono::steady_clock::now();
            }
        }
        void on_shutdown() override {
            if (auto e = w->finish()) throw std::runtime_error(*e);
        }
    };

    explicit Pipeline(PipelineBuilder<C>& b) {
        if (b.checkpoint_every_ && !b.journal_) throw Error(Error::Kind::Config, "checkpoint_every needs journals");
        if (!b.map_) {
            PartitionMap m;
            if (auto e = PartitionMap::make(b.partitions_, {}, m)) throw Error(Error::Kind::Config, *e);
            b.map_ = std::move(m);
        }
        map_ = *b.map_;
        const std::uint32_t P = map_.partitions();
        for (std::size_t n : {b.ingress_, b.inbox_, b.outbox_})
            if (n < 2 || (n & (n - 1)) != 0) throw Error(Error::Kind::Config, "ring sizes must be powers of two >= 2");
        std::vector<C> cores;
        std::uint64_t next_iseq = 1;
        if (b.initial_) {
            if (b.initial_->cores.size() != P) throw Error(Error::Kind::Config, "recovered cores != partitions");
            cores = std::move(b.initial_->cores);
            next_iseq = std::max<std::uint64_t>(b.initial_->next_iseq, 1);
        } else {
            for (std::uint32_t p = 0; p < P; ++p) cores.emplace_back(b.book_);
        }
        const bool journaled = b.journal_.has_value();
        const std::uint64_t start_wm = next_iseq - 1;
        sh_ = std::make_shared<detail::Shared>();
        sh_->partitions = P;
        sh_->book = b.book_;
        sh_->timestamps = b.timestamps_;
        sh_->egress_epoch.reset(new detail::Padded[P]);
        sh_->journal = b.journal_;
        for (std::uint32_t p = 0; p < P; ++p) {
            sh_->flushed.push_back(std::make_shared<std::atomic<std::uint64_t>>(start_wm));
            sh_->durable.push_back(std::make_shared<std::atomic<std::uint64_t>>(journaled ? start_wm : UINT64_MAX));
            sh_->io.push_back(std::make_shared<IoStats>());
            sh_->counters.push_back(std::make_shared<EngineCounters>());
        }

        // journals first, so I/O errors surface from build()
        std::vector<std::unique_ptr<ChunkWriter>> cmd_w(P), evt_w(P);
        if (journaled) {
            const auto& jc = *b.journal_;
            try {
                fs::create_directories(jc.dir);
                if (!jc.append) clear_journal_dir(jc.dir, jc.format);
                for (std::uint32_t p = 0; p < P; ++p) {
                    cmd_w[p] = std::make_unique<ChunkWriter>(open_journal(jc, Kind::Cmd, p, P, b.book_), jc.fsync,
                                                             Marks{sh_->flushed[p], sh_->durable[p], sh_->io[p]});
                    if (jc.events)
                        evt_w[p] = std::make_unique<ChunkWriter>(
                            open_journal(jc, Kind::Evt, p, P, b.book_), std::nullopt,
                            Marks{std::make_shared<std::atomic<std::uint64_t>>(0), std::make_shared<std::atomic<std::uint64_t>>(0)});
                }
            } catch (const std::exception& e) {
                throw Error(Error::Kind::Io, e.what());
            }
        }

        std::vector<disruptor::SingleProducer<CmdMsg>> inboxes;
        std::size_t n_egress = std::min<std::size_t>(b.egress_threads_, P);
        std::vector<std::vector<EgressPart>> egress_groups(n_egress);
        auto factories = b.egress_;
        for (std::uint32_t p = 0; p < P; ++p) {
            disruptor::RingBuilder<CmdMsg> ib(b.inbox_);
            ib.consumer({}, b.waits_.engine);
            auto [inbox, inbox_cons] = ib.build_single();
            auto ictl = inbox.control();
            inbox_ctl_.push_back(ictl);
            sh_->add_alert([ictl]() mutable { ictl.alert(); });
            inboxes.push_back(std::move(inbox));

            disruptor::RingBuilder<EvtMsg> ob(b.outbox_);
            ob.consumer({}, b.waits_.egress);
            auto [outbox, outbox_cons] = ob.build_single();
            auto octl = outbox.control();
            outbox_ctl_.push_back(octl);
            sh_->add_alert([octl]() mutable { octl.alert(); });

            std::optional<JournalFormat> fmt;
            if (journaled) fmt = b.journal_->format;
            std::optional<Segmenter> seg;
            if (journaled) seg = Segmenter{b.journal_->dir, b.journal_->format, Kind::Cmd, p, P, b.book_};
            threads_.emplace_back(engine_thread, sh_, std::move(inbox_cons[0]), std::move(outbox),
                                  std::move(cores[p]), std::move(cmd_w[p]), fmt, seg, sh_->counters[p]);

            EgressCtx ctx{p, P, sh_->epoch, sh_->durable[p]};
            EgressPart part{p, std::move(outbox_cons[0]), {}, ctx};
            if (evt_w[p]) {
                auto ej = std::make_unique<EvtJournal>();
                ej->w = std::move(evt_w[p]);
                ej->f = b.journal_->format;
                ej->seg = Segmenter{b.journal_->dir, b.journal_->format, Kind::Evt, p, P, b.book_};
                part.plugs.push_back(std::move(ej));
            }
            for (auto& f : factories) part.plugs.push_back(f(ctx));
            egress_groups[p % n_egress].push_back(std::move(part));
        }
        for (auto& g : egress_groups)
            threads_.emplace_back(egress_thread, sh_, std::move(g), journaled);

        disruptor::RingBuilder<CmdMsg> rb(b.ingress_);
        rb.consumer({}, b.waits_.router);
        auto [ingress, rcons] = rb.build_multi();
        auto gctl = ingress.control();
        ingress_ctl_.emplace(gctl);
        sh_->add_alert([gctl]() mutable { gctl.alert(); });
        threads_.emplace_back(router_thread, sh_, std::move(rcons[0]), std::move(inboxes), map_, next_iseq);
        handle_ = std::make_unique<Handle>(std::move(ingress), sh_);
        if (b.checkpoint_every_) {
            auto interval = *b.checkpoint_every_;
            ckpt_ = std::thread([this, interval, h = *handle_]() mutable {
                std::unique_lock<std::mutex> g(ckpt_m_);
                for (;;) {
                    if (ckpt_cv_.wait_for(g, interval, [&] { return ckpt_stop_; })) return;
                    if (sh_->failed.load()) return;
                    g.unlock();
                    try {
                        checkpoint_on(h);
                    } catch (const Error& e) {
                        if (e.kind != Error::Kind::Closed && e.kind != Error::Kind::Failed)
                            sh_->fail(std::string("checkpoint: ") + e.what());
                        return;
                    } catch (const std::exception& e) {
                        sh_->fail(std::string("checkpoint: ") + e.what());
                        return;
                    }
                    g.lock();
                }
            });
        }
    }

    template <class F>
    static void guarded(detail::Shared& sh, const char* name, F&& body) {
        try {
            body();
        } catch (const std::exception& e) {
            sh.fail(std::string(name) + " thread failed: " + e.what());
        } catch (...) {
            sh.fail(std::string(name) + " thread failed");
        }
    }

    static void router_thread(std::shared_ptr<detail::Shared> sh, disruptor::Consumer<CmdMsg> ingress,
                              std::vector<disruptor::SingleProducer<CmdMsg>> inboxes, PartitionMap map,
                              std::uint64_t next_iseq) {
        guarded(*sh, "router", [&] {
            std::uint64_t iseq = next_iseq - 1;
            bool stop = false;
            while (!stop) {
                bool ok = ingress.wait_poll([&](const CmdMsg& m, std::int64_t, bool eob) {
                    if (m.ctl == Control::None) {
                        ++iseq;
                        inboxes[map.partition(m.symbol)].stage([&](CmdMsg& s) { s = m; s.iseq = iseq; });
                    } else {
                        for (auto& ib : inboxes) ib.stage([&](CmdMsg& s) { s = m; s.iseq = iseq; });
                        if (m.ctl == Control::Shutdown) stop = true;
                    }
                    if (eob)
                        for (auto& ib : inboxes) ib.commit();
                });
                if (!ok) break;
            }
            for (auto& ib : inboxes) ib.commit();
        });
    }

    static void engine_thread(std::shared_ptr<detail::Shared> sh, disruptor::Consumer<CmdMsg> inbox,
                              disruptor::SingleProducer<EvtMsg> out, C core, std::unique_ptr<ChunkWriter> journal,
                              std::optional<JournalFormat> fmt, std::optional<Segmenter> seg,
                              std::shared_ptr<EngineCounters> counters) {
        guarded(*sh, "engine", [&] {
            auto last_handoff = std::chrono::steady_clock::now();
            bool stop = false;
            std::uint64_t n_commands = 0, n_events = 0;
            for (;;) {
                bool force = false;
                std::size_t n = inbox.poll([&](const CmdMsg& m, std::int64_t, bool eob) {
                    if (m.ctl == Control::None) {
                        if (journal) {  // journal-before-apply, in-thread
                            journal->reserve(MAX_RECORD);
                            push_cmd(journal->buf(), *fmt, m.iseq, m.symbol, m.cmd);
                            journal->record(m.iseq);
                        }
                        const std::uint64_t iseq = m.iseq, t_pub = m.t_pub;
                        ++n_commands;
                        core.apply(m.symbol, m.cmd, [&](Symbol s, std::uint64_t seq, const Event& ev) {
                            ++n_events;
                            out.stage([&](EvtMsg& e) {
                                e.iseq = iseq; e.seq = seq; e.t_pub = t_pub; e.arg = 0;
                                e.symbol = s; e.ctl = Control::None; e.ev = ev;
                            });
                        });
                    } else {
                        // the new segment starts at this cut, before the snapshot is reported
                        if (m.ctl == Control::Checkpoint && journal && seg) seg->rotate(*journal, m.iseq);
                        if (m.ctl == Control::Snapshot || m.ctl == Control::Checkpoint) {
                            std::vector<Block> blocks;
                            core.snapshot_blocks(blocks);
                            {
                                std::lock_guard<std::mutex> g(sh->snaps_m);
                                auto it = sh->snaps.find(m.arg);
                                if (it != sh->snaps.end()) {
                                    auto& st = it->second;
                                    for (auto& bl : blocks) st.blocks.push_back(std::move(bl));
                                    st.cut = m.iseq;
                                    --st.remaining;
                                }
                            }
                            sh->snaps_cv.notify_all();
                        }
                        if (m.ctl == Control::Shutdown) {
                            stop = true;
                            if (journal)
                                if (auto e = journal->finish()) sh->fail("journal: " + *e);
                        } else {
                            force = true;
                        }
                        out.stage([&](EvtMsg& e) {
                            e.iseq = m.iseq; e.seq = 0; e.t_pub = 0; e.arg = m.arg;
                            e.symbol = 0; e.ctl = m.ctl;
                        });
                    }
                    if (eob) out.commit();
                });
                if (n) {
                    counters->commands.store(n_commands, std::memory_order_relaxed);
                    counters->events.store(n_events, std::memory_order_relaxed);
                }
                if (stop || inbox.is_alerted()) break;
                if (journal && journal->pending() &&
                    (force || (n == 0 && std::chrono::steady_clock::now() - last_handoff >= std::chrono::microseconds(50)))) {
                    journal->hand_off();
                    last_handoff = std::chrono::steady_clock::now();
                }
                if (n == 0) inbox.idle();
                else inbox.reset_idle();
            }
            out.commit();
        });
    }

    static void egress_thread(std::shared_ptr<detail::Shared> sh, std::vector<EgressPart> parts, bool journaled) {
        guarded(*sh, "egress", [&] {
            std::size_t idle_on = 0;
            for (;;) {
                std::size_t total = 0, live = 0;
                for (std::size_t i = 0; i < parts.size(); ++i) {
                    auto& ep = parts[i];
                    if (ep.stopped) continue;
                    ++live;
                    idle_on = i;
                    bool stop = false;
                    std::size_t n = ep.outbox.poll([&](const EvtMsg& m, std::int64_t, bool eob) {
                        if (m.ctl == Control::None) {
                            ep.last_iseq = m.iseq;
                            for (auto& pl : ep.plugs) pl->on_event(m);
                        } else if (m.ctl == Control::Barrier) {
                            for (auto& pl : ep.plugs) { pl->on_batch_end(); pl->on_idle(); }
                            sh->egress_epoch[ep.p].v.store(m.arg, std::memory_order_release);
                        } else if (m.ctl == Control::Shutdown) {
                            stop = true;
                        } else if (m.ctl == Control::Checkpoint) {
                            for (auto& pl : ep.plugs) pl->on_checkpoint(m.iseq);
                        }
                        if (eob)
                            for (auto& pl : ep.plugs) pl->on_batch_end();
                    });
                    total += n;
                    if (stop) {
                        if (journaled) {
                            auto d = ep.ctx.durable;
                            std::uint64_t last = ep.last_iseq;
                            detail::wait_until(*sh, [&] { return d->load(std::memory_order_acquire) >= last; });
                        }
                        for (auto& pl : ep.plugs) { pl->on_idle(); pl->on_shutdown(); }
                        ep.stopped = true;
                    } else if (n == 0) {
                        for (auto& pl : ep.plugs) pl->on_idle();
                    }
                }
                if (live == 0) return;
                bool alerted = false;
                for (auto& ep : parts) alerted |= ep.outbox.is_alerted();
                if (alerted) return;
                if (total == 0) parts[idle_on].outbox.idle();
                else parts[idle_on].outbox.reset_idle();
            }
        });
    }

    std::shared_ptr<detail::Shared> sh_;
    PartitionMap map_;
    std::vector<std::thread> threads_;
    std::unique_ptr<Handle> handle_;
    bool shut_ = false;
    std::thread ckpt_;
    std::optional<disruptor::RingControl<CmdMsg>> ingress_ctl_;
    std::vector<disruptor::RingControl<CmdMsg>> inbox_ctl_;
    std::vector<disruptor::RingControl<EvtMsg>> outbox_ctl_;
    std::mutex ckpt_m_;
    std::condition_variable ckpt_cv_;
    bool ckpt_stop_ = false;
};

}  // namespace orderer
