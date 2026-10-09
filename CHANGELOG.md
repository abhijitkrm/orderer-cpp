# Changelog

## Unreleased: orderer-spec/1.2

- Binary journals are version 2 (CRC-32C per record); version 1 still reads.
- `repair_dir` / `orderrecover --repair` truncate a torn final record.
- `Pipeline::checkpoint()` rotates journals onto segments at a clean cut,
  writes the snapshot durably and removes covered segments.
- `PipelineBuilder::checkpoint_every(interval)`: automatic checkpoints from
  a background thread with its own `Handle` (stopped first at shutdown).
- `orderrun --checkpoint-every K` and `--durable`; ctest runs the vendored
  `spec/conformance.sh`.

## Unreleased: v0.1.0 candidate (implements orderer-spec/1.1)

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
