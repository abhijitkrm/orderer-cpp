// stats.hpp — operational statistics (not part of the spec contract): ring
// depths, per-partition counters, journal watermarks and fsync timings, and
// a Prometheus text-format rendering.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace orderer {

/// fsync timings one journal I/O thread records.
struct IoStats {
    std::atomic<std::uint64_t> fsyncs{0}, fsync_ns_total{0}, fsync_ns_max{0};
    void record(std::uint64_t ns) {
        fsyncs.fetch_add(1, std::memory_order_relaxed);
        fsync_ns_total.fetch_add(ns, std::memory_order_relaxed);
        auto m = fsync_ns_max.load(std::memory_order_relaxed);
        while (ns > m && !fsync_ns_max.compare_exchange_weak(m, ns, std::memory_order_relaxed)) {}
    }
};

/// Counters an engine thread publishes once per batch.
struct EngineCounters {
    std::atomic<std::uint64_t> commands{0}, events{0};
};

struct PartitionStats {
    std::uint32_t partition = 0;
    std::uint64_t inbox_depth = 0, outbox_depth = 0;  // routed / staged, not yet consumed
    std::uint64_t commands = 0, events = 0;
    std::uint64_t flushed_iseq = 0, durable_iseq = 0;  // UINT64_MAX without journals
    std::uint64_t fsyncs = 0, fsync_ns_total = 0, fsync_ns_max = 0;
};

/// A point-in-time view (fields individually exact, not mutually consistent).
struct PipelineStats {
    std::uint64_t ingress_depth = 0;  // published, not yet routed
    std::vector<PartitionStats> partitions;

    /// Prometheus text exposition format (version 0.0.4).
    std::string to_prometheus() const {
        std::string out;
        auto series = [&](const char* name, const char* help, const char* kind, auto get) {
            out += std::string("# HELP orderer_") + name + " " + help + "\n# TYPE orderer_" + name + " " + kind + "\n";
            for (auto& p : partitions)
                out += std::string("orderer_") + name + "{partition=\"" + std::to_string(p.partition) + "\"} " +
                       std::to_string(get(p)) + "\n";
        };
        out += "# HELP orderer_ingress_depth Commands published, not yet routed.\n# TYPE orderer_ingress_depth gauge\n";
        out += "orderer_ingress_depth " + std::to_string(ingress_depth) + "\n";
        series("inbox_depth", "Commands routed, not yet applied.", "gauge", [](auto& p) { return p.inbox_depth; });
        series("outbox_depth", "Events staged, not yet consumed by egress.", "gauge", [](auto& p) { return p.outbox_depth; });
        series("commands_total", "Commands applied.", "counter", [](auto& p) { return p.commands; });
        series("events_total", "Events emitted.", "counter", [](auto& p) { return p.events; });
        series("durable_iseq", "Highest iseq covered by a completed fsync.", "gauge", [](auto& p) { return p.durable_iseq; });
        series("fsyncs_total", "Journal fsyncs.", "counter", [](auto& p) { return p.fsyncs; });
        series("fsync_ns_total", "Time spent in journal fsync, in nanoseconds.", "counter", [](auto& p) { return p.fsync_ns_total; });
        series("fsync_max_ns", "Longest journal fsync, in nanoseconds.", "gauge", [](auto& p) { return p.fsync_ns_max; });
        return out;
    }
};

}  // namespace orderer
