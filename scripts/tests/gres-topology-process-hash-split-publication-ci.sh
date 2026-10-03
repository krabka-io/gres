#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export KRABKA_G8_SPLIT_WORKLOAD=hash
export KRABKA_G8_SPLIT_EVIDENCE_ROOT="$PWD/target/g9-hash-split-crash"
exec scripts/tests/gres-topology-process-split-publication-ci.sh
