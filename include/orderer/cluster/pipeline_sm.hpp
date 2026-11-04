// pipeline_sm.hpp — the orderer pipeline as the replicated state machine
// (port of orderer-rust's pipeline_sm.rs). A committed log entry carries a
// batch of commands; applying it publishes the batch into the local Pipeline
// in order, so every replica's partitions see the same ingress order and emit
// byte-identical events (spec/PIPELINE.md §3). The pipeline runs without
// journaling: the replicated log is the journal. The state serialised for
// snapshots and digests is `iseq u64 | partitions u32 | matcher-snap/1 body`,
// the same bytes orderer-rust produces, so replicas of the two languages
// agree on digests.
#pragma once

#include <functional>

#include "../pipeline.hpp"
#include "../recover.hpp"
#include "state_machines.hpp"

namespace orderer::cluster {

template <class C = FifoCore>
class PipelineSm : public StateMachine {
public:
    /// Builds a pipeline, optionally from restored state. It must configure
    /// `book` and `map`'s partition count, and no journaling.
    using Build = std::function<std::unique_ptr<Pipeline<C>>(std::optional<Initial<C>>)>;

    PipelineSm(BookConfig book, PartitionMap map, Build build) : book_(book), map_(std::move(map)), build_(std::move(build)) {
        pipeline_ = build_(std::nullopt);
    }
    ~PipelineSm() override { if (pipeline_) { try { pipeline_->shutdown(); } catch (...) {} } }

    /// A ready-made pipeline with a counting (non-accumulating) egress.
    static std::unique_ptr<PipelineSm> counting(std::uint32_t partitions, BookConfig book = BookConfig{}) {
        PartitionMap map;
        if (auto e = PartitionMap::make(partitions, {}, map)) throw std::runtime_error(*e);
        return std::make_unique<PipelineSm>(book, map, [partitions, book](std::optional<Initial<C>> init) {
            auto [factory, handle] = metrics(0);
            auto b = Pipeline<C>::builder();
            b.book_config(book).partitions(partitions).egress(std::move(factory)).timestamps(false);
            if (init) b.initial(std::move(*init));
            return b.build();
        });
    }

    void apply(Index, const Entry& e) override {
        std::vector<CmdSym> cmds;
        try { cmds = decode_batch(e.data.data(), e.data.size()); }
        catch (const CodecError&) { ++bad_entries_; return; }  // every replica skips it identically
        if (cmds.empty()) return;
        if (pipeline_->publish_batch(cmds.data(), cmds.size()) != orderer::Status::Ok) throw std::runtime_error("pipeline rejected committed commands");
    }

    std::vector<std::uint8_t> snapshot(Index) override { return encode_state(); }

    std::optional<std::string> restore(Index, const std::uint8_t* d, std::size_t n) override {
        if (n < 12) return "short pipeline snapshot";
        Snapshot snap;
        std::memcpy(&snap.iseq, d, 8);
        std::memcpy(&snap.partitions, d + 8, 4);
        snap.body.assign(reinterpret_cast<const char*>(d + 12), n - 12);
        try {
            auto [cfg, cores] = orderer::restore<C>(book_, map_, &snap);
            (void)cfg;
            if (pipeline_) pipeline_->shutdown();
            Initial<C> init;
            init.cores = std::move(cores);
            init.next_iseq = snap.iseq + 1;
            pipeline_ = build_(std::move(init));
        } catch (const std::exception& e) { return e.what(); }
        return std::nullopt;
    }

    std::optional<Digest> digest(Index) override { auto b = encode_state(); return sha256(b); }
    std::uint64_t bad_entries() const { return bad_entries_; }

private:
    std::vector<std::uint8_t> encode_state() {
        pipeline_->drain();  // egress has seen everything before the snapshot is taken
        Snapshot s = pipeline_->snapshot();
        std::vector<std::uint8_t> out;
        out.reserve(12 + s.body.size());
        put_u64(out, s.iseq);
        put_u32(out, s.partitions);
        out.insert(out.end(), s.body.begin(), s.body.end());
        return out;
    }

    BookConfig book_;
    PartitionMap map_;
    Build build_;
    std::unique_ptr<Pipeline<C>> pipeline_;
    std::uint64_t bad_entries_ = 0;
};

}  // namespace orderer::cluster
