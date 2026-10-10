# orderer-cpp

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The C++20 implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine around the
[matcher](https://github.com/abhijitkrm/matcher) order book. It is
header-only, has no dependencies, and implements `orderer-spec/1.3`. It is
a port of [orderer-rust](https://github.com/abhijitkrm/orderer-rust), and
**byte-identical** to it: listings, per-partition journals (JSONL and
binary), snapshots and exit codes.

```
Handle::publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
 (any threads)    (multi-     (iseq,  │              (journal +
                   producer)   route) └─▶ …            apply)
```

## Install

Header-only. Either install the CMake package:

```bash
cmake -S . -B build && cmake --install build --prefix /usr/local
```

```cmake
find_package(orderer 0.2 CONFIG REQUIRED)
target_link_libraries(app PRIVATE orderer::orderer)
```

or vendor it with `FetchContent` (tag `v0.2.1`) and link `orderer`.

## Quick start

```cpp
#include <orderer/orderer.hpp>
using namespace orderer;

JournalConfig journal;
journal.dir = dir;                       // binary, durable: fsync every 1024 records
auto [events_plug, events] = collect(true);
auto p = Pipeline<FifoCore>::builder()
             .partitions(2)
             .journal(journal)
             .egress(events_plug)        // or acks(...), metrics(...), callback(...), your own Egress
             .build();

Handle h = p->handle();                  // copyable; publish from any thread
h.publish(7, Command::new_limit(1, Side::Ask, 100, 10, Tif::Gtc));
h.publish(7, Command::new_limit(2, Side::Bid, 100, 4, Tif::Gtc));
p->drain();                              // applied and delivered
p->snapshot().write(dir / "books.snap"); // consistent cut: matcher-snap/1 + .meta
p->shutdown();
```

The full version is `examples/quickstart.cpp` (CMake target `quickstart`).

## Plug points

| Seam | Type | Built-ins |
|---|---|---|
| Matching core | `MatchingCore` concept | `FifoCore` (matcher-cpp `Engine`), `NoopCore` |
| Egress | `Egress` + `EgressFactory` (one per partition) | `collect`, `callback`, `acks` (durability-gated), `metrics` |
| Routing | `PartitionMap` | hash (spec/ROUTING.md) + table overrides |
| Journals | `JournalConfig`, `FsyncPolicy` | JSONL or binary, group-commit fsync on I/O threads |
| Waiting | `Waits` / `disruptor::WaitStrategy` | BusySpin, Yield, Backoff, Blocking |
| Recovery | `recover<C>(…)`, `read_snapshot`, `restore<C>`, `repair_dir` | snapshot + journals → cores at any P; torn tails repaired |
| Observability | `Pipeline::stats()`, `PipelineStats::to_prometheus()` | ring depths, counters, watermarks, fsync timings |
| Checkpoints | `Pipeline::checkpoint()` | durable snapshot + journal segment rotation; old segments removed |

## Build, test, harness

```bash
cmake -S . -B build && cmake --build build -j && (cd build && ctest)   # = scripts/test.sh
cmake -S . -B build-thread -DORDERER_SANITIZE=thread                    # or address
scripts/build-harness.sh            # → harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}
CHECKED=1 scripts/build-harness.sh  # the same, under ASan + UBSan
scripts/vendored.sh                 # vendored spec/ + vectors/ untouched
```

The suites:

- `ring`: the disruptor protocol, ported from orderer-rust.
- `golden`: every matcher vector and orderer regression vector through the
  pipeline at P=1 and P=4, in both index modes, plus the routing vectors.
- `pipeline`: partitions, recovery, controls, durability, backpressure,
  plugs.
- `no_alloc`: zero steady-state allocations.
- `conformance`: the vendored `spec/conformance.sh`, every orderer vector
  through the binaries, byte-exact (checkpoints, repair and version-1
  journals included).

Cross-implementation proofs (diffuzz, exhaustive, e2e, snapdiff) run from
the spec repo with this repo checked out next to it.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the C++-specific design, and
orderer-rust's DESIGN.md for the architecture.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
