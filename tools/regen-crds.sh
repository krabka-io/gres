#!/usr/bin/env bash
set -euo pipefail
cargo run -p krabka-gres-operator -- gen-crds deploy/crds
echo "Regenerated. Review the diff with: git diff deploy/crds"
