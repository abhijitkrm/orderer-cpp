# DESIGN — orderer-cpp

orderer-cpp is a port of [orderer-rust](https://github.com/abhijitkrm/orderer-rust)
to C++20, header-only like matcher-cpp. The architecture, rationale and
measurements are orderer-rust's `docs/DESIGN.md`; this note records only
what is C++-specific. As in every port, the bytes are contract and the
threading is not: listings, journals (both encodings), snapshots, routing
and exit codes are byte-identical to orderer-rust. The spec repo's
`diffuzz`, `exhaustive`, `e2e` and `snapdiff` scripts prove it.

## Layout

| Header | Role | orderer-rust counterpart |
|---|---|---|
| `orderer/disruptor.hpp` | ring, sequences, single/multi producers, consumers (barriers), wait strategies, builder | `orderer-disruptor` |
| `orderer/core.hpp` | the `MatchingCore` concept, `FifoCore`, `NoopCore`, strict snapshot parsing and validation | `orderer-core/src/core.rs`, `snapshot.rs` |
| `orderer/flat.hpp` | strict flat-JSON field access, canonical command lines | `orderer-core/src/jsonflat.rs` |
| `orderer/routing.hpp` | `hash_partition`, `PartitionMap` | `orderer/src/routing.rs` |
| `orderer/journal.hpp` | encodings, strict readers, `ChunkWriter` + I/O thread | `journal.rs`, `writer.rs` |
| `orderer/egress.hpp` | slot types, `Egress`, `collect` / `callback` / `acks` / `metrics` | `msg.rs`, `egress.rs` |
| `orderer/pipeline.hpp` | builder, `Pipeline`, `Handle`, threads, controls | `pipeline.rs` |
| `orderer/recover.hpp` | snapshot restore, journal merge, replay | `recover.rs` |
| `orderer/harness.hpp` | corpus loading, args, `run_corpus` | `harness.rs` |
| `include/matcher/` | matcher-cpp, vendored verbatim | `orderer-core` (matcher-rust) |

## C++ specifics

- **Ring protocol.** It is the same as orderer-rust (`docs/DESIGN.md` §3
  there): writes only under an unpublished claim, reads only after an
  acquire of the cursor or availability flag. Slots are a plain `T[]`, and
  `std::atomic` with acquire/release orders them. Verification: CI runs
  the suite under **ThreadSanitizer** and **ASan+UBSan**; `ring_test`
  stress-runs 50/50 in release and 30/30 under TSan.
- **Errors.** Construction, control and I/O failures throw
  `orderer::Error` (`Closed`, `Full`, `Failed`, `Config` or `Io`).
  `publish` returns a `Status`. A throwing thread (core, plug, journal)
  marks the pipeline failed and alerts every ring, so `drain`, `snapshot`
  and `shutdown` throw `Error(Failed)` instead of hanging.
- **Journaling** is inline only: the engine thread encodes each record
  before applying. This was orderer-rust's measured-better default.
  `ChunkWriter`'s queues are fixed-capacity arrays, and the I/O thread uses
  POSIX `write` plus `F_FULLFSYNC` (macOS) or `fdatasync` (Linux).
- **Zero allocation.** `no_alloc_test` replaces global `operator new` and
  counts allocations across 1M commands: zero. This needed the matcher-cpp
  `Engine` fix (`5ddb7d2`): `submit_tagged` used to build a `std::function`
  and a whole `OrderBook` temporary on every call.
- **Strict input.** matcher-cpp's `jsonflat` turns malformed fields into 0.
  `orderer::flat` rejects them, exactly as matcher-rust's parser does, so
  harness exit codes match orderer-rust's.
- **Checked builds.** `CHECKED=1 scripts/build-harness.sh` builds the
  harness with ASan+UBSan. The spec repo's diffuzz and exhaustive scripts
  run it that way. matcher-cpp has no book invariant checker, so sanitizers
  stand in for orderer-rust's debug `check_invariants`.

## Building

```bash
cmake -S . -B build && cmake --build build -j && (cd build && ctest)
cmake -S . -B build-thread -DORDERER_SANITIZE=thread   # or address
```

Requires C++20 (`<barrier>`, concepts). Tested with Apple clang 14 and GCC
12+.
