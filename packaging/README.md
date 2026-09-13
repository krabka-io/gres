# Gres container images

This directory holds the image inputs for the two Gres server binaries. The
build uses no Dockerfile for the published images. Chainguard's
[melange](https://github.com/chainguard-dev/melange) compiles the APK packages,
and [apko](https://github.com/chainguard-dev/apko) assembles the OCI images.

| Image | Package recipe | Image contents |
| --- | --- | --- |
| `crabka-gres` | [`melange/crabka.yaml`](melange/crabka.yaml) | [`apko/crabka-gres.yaml`](apko/crabka-gres.yaml) |
| `crabka-gres-activator` | [`melange/crabka.yaml`](melange/crabka.yaml) | [`apko/crabka-gres-activator.yaml`](apko/crabka-gres-activator.yaml) |

One `melange build` compiles both binaries and splits them into two
subpackages. Each apko config installs one subpackage on a Wolfi base and runs
it as the `nonroot` user.

[`docker/Dockerfile.local-binary`](docker/Dockerfile.local-binary) wraps a
locally built binary. `scripts/gres-kind-lifecycle.sh` uses it to load images
into a disposable Kind cluster.

No workflow in this repository publishes these images yet.
