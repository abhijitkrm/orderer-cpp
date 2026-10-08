# Contributing

orderer-cpp implements the contract in the
[orderer spec repo](https://github.com/abhijitkrm/orderer). Read its
CONTRIBUTING first.

- `spec/`, `vectors/` and `include/matcher/` are vendored. Never edit them
  here (`docs/VENDORED.md`). Spec changes land in the spec repo; matching
  semantics change only through matcher.
- Anything observable (stdout, journals, snapshots, routing, exit codes)
  must stay byte-identical to orderer-rust. Run the spec repo's
  `scripts/diffuzz.sh`, `exhaustive.sh`, `e2e.sh` and `snapdiff.sh` with
  this repo checked out as `../orderer-cpp`.
- Before sending a change: `scripts/test.sh`, plus the suite under
  `-DORDERER_SANITIZE=address` and `=thread`.
