# Changelog

## Unreleased

- `EgressPlacement::Inline` (`PipelineBuilder::egress_placement`, default `Thread`):
  the engine thread runs the egress plugs itself, with no outbox ring, egress
  thread or per-event copy. Journals must be off (`Error::Kind::Config`
  otherwise). Same events, same order (`pipeline_test`). With the protocol's
  instrumentation, M1, real core P=3: 27.6M -> 34.0M; neutral or slower at other
  P; no-op core P=2 without instrumentation: 68M -> 101M.
- Fix: `Pipeline::stats()` read a per-partition outbox ring that inline egress does not
  have; now 0, with a test.
- `orderbench --latency on|off` (off stops stamping and sampling commands, two
  clock reads per command; default on) and `--egress thread|inline`. Journals-off
  pipeline without the instrumentation: real core P=3 28M -> 40M, no-op P=2
  47M -> 67M.

## 0.2.1 (orderer-spec/1.3)

- Vendors matcher-cpp 55091ea: the price ladder finds the next best price through a
  summary bitmap, and an emptied side resets at once. A book whose side
  emptied used to scan the whole ladder per cancel (W3 at 1M ops: 4-24x
  faster in the matcher bench). Output unchanged.
- Repair (orderer-spec 1.3), for three kinds of damage SIGKILL left that
  `--repair` refused: binary tails of all-zero records (an interrupted
  write on macOS can extend a file with zeros, a whole write buffer of
  them) are cut; a segment created at a checkpoint whose header never
  arrived is deleted; and when the last segment holds no records, the
  torn segment before it is repaired too.

## 0.2.0 (orderer-spec/1.2)

- Binary journals are version 2 (CRC-32C per record); version 1 still reads.
- `repair_dir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline::checkpoint()` rotates journals onto segments at a clean cut,
  writes the snapshot durably and removes covered segments.
- `Pipeline::stats()` (stats.hpp): ring depths, per-partition counts,
  watermarks, fsync timings; `PipelineStats::to_prometheus()`.
- `PipelineBuilder::checkpoint_every(interval)`: automatic checkpoints from
  a background thread with its own `Handle` (stopped first at shutdown).
- `orderrun --checkpoint-every K` and `--durable`; ctest runs the vendored
  `spec/conformance.sh`.

## 0.1.0 (orderer-spec/1.1)

- Port of orderer-rust to header-only C++20. It is byte-identical across
  listings, JSONL and binary journals at every P, snapshots, recovery and
  harness exit codes. Proven by the spec repo's diffuzz (8 seeds),
  exhaustive (depth 3), e2e (63-way cross-restore) and snapdiff, against
  orderer-rust and all five matcher ports.
- Vendors matcher-cpp at 5ddb7d2, which includes two fixes upstreamed while
  building this port: the OrderMap deletion bug, and `Engine` constructing
  a whole book (and a `std::function`) on every command.
- Disruptor ring (single and multi producers, availability flags, CAS
  `try_publish`, barriers, four wait strategies), pipeline with inline
  journal-before-apply, chunked I/O-thread journaling with group-commit
  fsync, durability-gated acks, drain/snapshot/shutdown controls, recovery
  at any P, egress plugs, five harnesses.
- Tests pass under ASan+UBSan and ThreadSanitizer. Zero steady-state
  allocations.
