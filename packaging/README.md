# Gres container images

This directory holds the published Gres server image. The build uses no
Dockerfile: Bazel compiles `crabka-gres`, apko assembles locked Chainguard
Wolfi bases, and rules_img adds the matching binary and publishes the OCI
manifest or multi-platform release index.

| Image | Binary | Image contents |
| --- | --- | --- |
| `ghcr.io/krabka-io/gres` | `//crates/gres:crabka-gres` | [`apko/crabka-gres.yaml`](apko/crabka-gres.yaml) |

[`docker/Dockerfile.local-binary`](docker/Dockerfile.local-binary) wraps a
locally built binary. `scripts/gres-kind-lifecycle.sh` uses it to load images
into a disposable Kind cluster.

Main publishes an AMD64 delivery image tagged by commit. Release tags publish
one OCI index for linux/amd64 and linux/arm64, with version, commit, and latest
tags.
