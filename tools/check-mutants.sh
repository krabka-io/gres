#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 //crates/name:name_mutants SHARD_INDEX SHARD_COUNT" >&2
  exit 2
fi

export TEST_SHARD_INDEX="$2"
export TEST_TOTAL_SHARDS="$3"
exec bazel run --test_sharding_strategy=disabled "$1"
