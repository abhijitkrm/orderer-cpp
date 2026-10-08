// Sink seam + stock sinks. apply() is templated on Sink — any object with
// `void on_event(uint64_t seq, const Event&)` works (concept-checked).
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.hpp"

namespace matcher {

template <class S>
concept Sink = requires(S& s, std::uint64_t seq, const Event& ev) {
    { s.on_event(seq, ev) };
};

// No-op for benchmarks — folds content so calls aren't elided.
struct NullSink {
    std::uint64_t acc = 0;
    inline void on_event(std::uint64_t seq, const Event& ev) { acc += seq ^ ev.fold(); }
};

// Records events for tests/replay.
struct VecSink {
    std::vector<std::pair<std::uint64_t, Event>> events;
    void on_event(std::uint64_t seq, const Event& ev) { events.emplace_back(seq, ev); }
};

// Canonical JSON lines (golden harness / text journal).
struct LinesSink {
    std::vector<std::string> lines;
    std::string scratch;
    void on_event(std::uint64_t seq, const Event& ev) {
        scratch.clear();
        write_canonical(seq, ev, scratch);
        lines.push_back(scratch);
    }
};

} // namespace matcher
