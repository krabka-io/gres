#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

readonly family=publication
readonly evidence_dir="${KRABKA_G8_SPLIT_EVIDENCE_ROOT:-$PWD/target/g8-split-crash}/$family"
readonly validator="$PWD/scripts/tests/validate-gres-split-crash-evidence.py"
readonly cases=(tenant_cas_before_journal_cas layout_published_after_journal_cas)

cargo build --locked -p krabka-cli -p krabka-gres
cargo test --locked -p krabka-gres --test topology_process_split_crash --no-run
rm -rf "$evidence_dir"
mkdir -p "$evidence_dir"
for case_name in "${cases[@]}"; do
  evidence="$evidence_dir/$case_name.json"
  KRABKA_G8_SPLIT_CRASH=1 KRABKA_G8_SPLIT_WORKLOAD="${KRABKA_G8_SPLIT_WORKLOAD:-ordinary}" KRABKA_G8_SPLIT_KILL_POINT="$case_name" \
    KRABKA_G8_SPLIT_CRASH_EVIDENCE="$evidence" \
    timeout 240s cargo test --locked -p krabka-gres --test topology_process_split_crash \
      -- --exact real_process_split_crash_anywhere --nocapture
  python3 "$validator" --validate-file "$family" "$case_name" "$evidence"
done
python3 "$validator" --validate-family "$family" "$evidence_dir"
