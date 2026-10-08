# krabka-gres-operator

Kubernetes operator for Crabka Gres fleets and tenants.

Part of [Crabka](https://github.com/krabka-io/gres), a Rust implementation of Apache Kafka.

## Overview

`krabka-gres-operator` reconciles the two `krabka.io/v1alpha1` Gres resources.
It manages only Gres. The Kafka cluster that stores the Gres registry and WAL
topics can come from any source, for example the `krabka-operator` `Kafka`
CRD or an external cluster. A `Gres` names that cluster's bootstrap servers
itself, so the operator never reads a `Kafka` CR.

- `Gres` (`gg`) is one PgDog-backed front door. Its spec holds the Kafka
  connection (`spec.kafka`), the PgDog and wake-activator settings, the tenant
  compute policy, the tenant defaults, the dry-run balancer knobs, and the
  optional OTLP tracing block.
- `GresTenant` (`gt`) is one tenant of a `Gres` fleet. Its spec names the
  fleet, the SQL user, the password Secret key, the suspension state, the
  resources, the range layout, and the default overrides.

The `Gres` reconciler renders `pgdog.toml` and `users.toml` from the live
tenants into a Secret, manages the PgDog and activator Services and
Deployments, and reloads PgDog through its admin endpoint. The `GresTenant`
reconciler creates the tenant WAL and config topics, writes the tenant record
to the `__gres_tenants` registry, manages the tenant's Kafka SCRAM credential
and ACLs, deploys one `krabka-gres` compute per range, and drives range splits
and WAL parking.

## Quick Start

1. Install the CRDs:

   ```bash
   krabka-gres-operator gen-crds deploy/crds
   kubectl apply -f deploy/crds/krabka.io_greses.yaml -f deploy/crds/krabka.io_grestenants.yaml
   ```

2. Run the operator against the current kube context:

   ```bash
   WATCH_NAMESPACES=gres krabka-gres-operator run
   ```

3. Create a fleet and a tenant, for example from
   [`sample/gres.yaml`](sample/gres.yaml):

   ```yaml
   apiVersion: krabka.io/v1alpha1
   kind: Gres
   metadata:
     name: analytics
   spec:
     kafka:
       bootstrapServers: demo-kafka-bootstrap.kafka.svc:9092
       sasl: false
     pgdog:
       replicas: 1
       listenPort: 6432
       adminSecretRef: { name: pgdog-admin, key: password }
   ```

### SASL Kafka listeners

On a listener that requires SASL/SCRAM-SHA-512, set `spec.kafka.sasl: true`
and point `spec.kafka.credentialsSecretRef` at a Secret with the operator's
own SCRAM credentials. The operator uses them for its admin and registry
connections, and the activator reads them from the same Secret. The
`usernameKey` and `passwordKey` keys default to `username` and `password`.
That principal must be able to manage topics, ACLs, and SCRAM credentials,
and to read and write `__gres_tenants`. Each tenant compute pod
authenticates with its own `gres-<tenant>` credential instead.

```yaml
spec:
  kafka:
    bootstrapServers: demo-kafka-bootstrap.kafka.svc:9092
    sasl: true
    credentialsSecretRef: { name: gres-operator-kafka }
```

## Configuration

Each option is a `run` flag and an environment variable. The flag wins.

| Option | Default | Description |
|--------|---------|-------------|
| `WATCH_NAMESPACES` | all namespaces | Comma-separated namespaces to watch. |
| `OPERATOR_NAMESPACE` | `krabka-gres-operator` | Namespace of the leader-election Lease. |
| `LEASE_NAME` | `krabka-gres-operator-leader` | Leader-election Lease name. |
| `HEALTH_ADDR` | `0.0.0.0:8080` | Address of `/healthz`, `/readyz`, and `/metrics`. |
| `DEFAULT_GRES_IMAGE` | compiled default | Compute image when `GresTenant.spec.image` is unset. |
| `DEFAULT_PGDOG_IMAGE` | compiled default | PgDog image when `Gres.spec.pgdog.image` is unset. |
| `DEFAULT_GRES_ACTIVATOR_IMAGE` | compiled default | Activator image when `Gres.spec.activator.image` is unset. |
| `TOPIC_MUTATION_TIMEOUT` | `30s` | Deadline of the tenant topic create and delete calls. |
| `GRES_CHECKPOINT_STORE` | unset | `s3` or `gcs`. Enables durable-checkpoint verification before WAL parking. |
| `GRES_CHECKPOINT_BUCKET` | unset | Bucket of the checkpoint manifests. |

`krabka-gres-operator run --help` lists the full set, including the PgDog
reload, requeue, and leader-election timings.

## Documentation

- [API Documentation](https://docs.rs/krabka-gres-operator)
- [Sample manifests](sample/gres.yaml)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
