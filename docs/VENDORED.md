# VENDORED — upstream sources

orderer-cpp vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `21bf70c8880e08226d978cacb531522fa7a03b68`
- **tag**: `orderer-spec/1.2` (draft)
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`include/matcher/` is [matcher-cpp](https://github.com/abhijitkrm/matcher-cpp)'s
`include/matcher/` at `5ddb7d2c2deb2e72b2d9a7fc09e27293bb2511b1`. That commit includes the two fixes found
while building orderer:

- `1285f6c`: OrderMap backward-shift deletion
- `5ddb7d2`: `Engine` no longer builds a book per command

To check it:

```bash
git -C ../matcher-cpp diff --stat 5ddb7d2c2deb2e72b2d9a7fc09e27293bb2511b1 -- include/matcher && diff -r ../matcher-cpp/include/matcher include/matcher
```

orderer's strict parsing (`include/orderer/flat.hpp`) wraps the core rather
than changing it. matcher-cpp's own `jsonflat` is lenient: malformed fields
become 0. The orderer harnesses must reject them (spec/HARNESS.md §5).
