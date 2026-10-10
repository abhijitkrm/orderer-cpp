# Changelog

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
