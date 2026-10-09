# VENDORED — upstream sources

orderer-cpp vendors two things, both **verbatim**. Never edit them here.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `6a46583cf1fc3aee1e5c2c935143b14d8d716067`
- **tag**: `orderer-spec/1.2`, plus matcher `06b5403` (bench docs only)
- **paths**: `spec=spec vectors=vectors`

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out,
against the pinned commit.

## 2. The matching core

`include/matcher/` is [matcher-cpp](https://github.com/abhijitkrm/matcher-cpp)'s
`include/matcher/` at `55091ea877d515b9ddf0c2c524367a526e7f45e1`. That commit includes three fixes found
while building orderer:

- `1285f6c`: OrderMap backward-shift deletion
- `5ddb7d2`: `Engine` no longer builds a book per command
- `55091ea`: the ladder rescans the next best price through a summary bitmap, and an emptied side resets the cursor at once (it used to scan the whole ladder).

To check it:

```bash
git -C ../matcher-cpp diff --stat 55091ea877d515b9ddf0c2c524367a526e7f45e1 -- include/matcher && diff -r ../matcher-cpp/include/matcher include/matcher
```

orderer's strict parsing (`include/orderer/flat.hpp`) wraps the core rather
than changing it. matcher-cpp's own `jsonflat` is lenient: malformed fields
become 0. The orderer harnesses must reject them (spec/HARNESS.md §5).
