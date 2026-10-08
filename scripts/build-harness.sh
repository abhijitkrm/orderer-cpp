#!/usr/bin/env bash
# build-harness.sh — spec/HARNESS.md §6: build the five tools and expose them
# as harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}.
#
#   scripts/build-harness.sh            # optimized (Release)
#   CHECKED=1 scripts/build-harness.sh  # AddressSanitizer + UBSan (HARNESS.md §4.2)
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${CHECKED:-0}" = 1 ]; then
  B=build-checked; ARGS=(-DCMAKE_BUILD_TYPE=RelWithDebInfo -DORDERER_SANITIZE=address)
else
  B=build; ARGS=(-DCMAKE_BUILD_TYPE=Release)
fi
cmake -S . -B "$B" "${ARGS[@]}" > /dev/null
cmake --build "$B" -j --target orderrun ordererfuzz orderrecover ordersnap orderbench > /dev/null
mkdir -p harness/bin
for t in orderrun ordererfuzz orderrecover ordersnap orderbench; do
  ln -sf "../../$B/$t" "harness/bin/$t"
done
