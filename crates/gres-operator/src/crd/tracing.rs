//! Distributed-tracing configuration shared by the Gres fleet.

use krabka_units::{Time, convert::TimeExt as _};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Fleet-wide distributed-tracing configuration for `Gres.spec.tracing`.
///
/// It has the shape of `Kafka.spec.tracing` in `krabka-operator` and maps
/// to the same `KRABKA_OTLP_*` env-var contract. The operator renders one
/// env entry per filled-in field onto every compute pod of the fleet, and the
/// telemetry pipeline of `krabka-gres` reads them from the environment at
/// startup.
///
/// The `type` discriminator is reserved for future tracing backends. Today
/// only `Otlp` is meaningful, and the matching `otlp` block is required when
/// `type = Otlp`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tracing {
    /// Tracing backend selector.
    #[serde(rename = "type")]
    pub kind: TracingType,
    /// OTLP-backend tuning. Required when `kind == Otlp`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp: Option<OtlpTracing>,
}

/// The tracing backends the operator knows how to render.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TracingType {
    /// OpenTelemetry OTLP exporter. Pair it with [`Tracing::otlp`] for the
    /// endpoint, the protocol, and the sampling.
    #[default]
    Otlp,
}

/// OTLP-specific tracing parameters. The operator renders each filled-in
/// field as a separate env var on every pod of the owning fleet.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OtlpTracing {
    /// Required. OTLP collector endpoint in the form `scheme://host:port`.
    /// The operator renders it as `KRABKA_OTLP_ENDPOINT`. A set field also
    /// sets `KRABKA_OTLP_ENABLED=true`.
    pub endpoint: String,
    /// Optional protocol. An unset field leaves the binary's own default of
    /// `Grpc`, which matches the OpenTelemetry SDK convention. The operator
    /// renders it as `KRABKA_OTLP_PROTOCOL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<OtlpProtocol>,
    /// Optional sampling ratio in `[0.0, 1.0]`. The operator renders it as
    /// `KRABKA_OTLP_SAMPLE_RATIO`. An unset field leaves the binary's own
    /// default of `1.0`, which samples every trace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_ratio: Option<f64>,
    /// Optional `service.name` resource attribute. The operator renders it as
    /// `OTEL_SERVICE_NAME`. An unset field leaves the binary's own name, which
    /// is `"krabka-gres"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// Optional export timeout. The operator renders it as
    /// `KRABKA_OTLP_TIMEOUT`. An unset field leaves the binary's own default
    /// of `10s`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "krabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub timeout: Option<Time>,
}

/// OTLP wire protocol selector. It has the same shape as the broker's
/// internal `OtlpProtocol` enum and the `OTEL_EXPORTER_OTLP_PROTOCOL` spec
/// values.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    /// gRPC over HTTP/2. This is the default, on port `:4317`.
    Grpc,
    /// HTTP/1 with a protobuf payload, on port `:4318`.
    HttpProtobuf,
}

impl OtlpProtocol {
    /// Render the env-var value that the broker's `OtlpProtocol::parse`
    /// expects.
    #[must_use]
    pub fn as_env_value(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpProtobuf => "http/protobuf",
        }
    }
}

impl Tracing {
    /// Shape-validate the tagged union.
    ///
    /// # Errors
    ///
    /// Fails when `type=Otlp` is missing the `otlp` block, when
    /// `otlp.endpoint` is empty, when `sampleRatio` is outside
    /// `[0.0, 1.0]`, or when `timeout` is not positive.
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, &self.otlp) {
            (TracingType::Otlp, None) => {
                Err("type=Otlp requires `otlp` (endpoint at minimum)".into())
            }
            (TracingType::Otlp, Some(otlp)) => {
                if otlp.endpoint.trim().is_empty() {
                    return Err("otlp.endpoint is required and must be non-empty".into());
                }
                if let Some(r) = otlp.sample_ratio
                    && !(0.0..=1.0).contains(&r)
                {
                    return Err(format!("otlp.sampleRatio must be in [0.0, 1.0] (got {r})"));
                }
                if let Some(s) = otlp.service_name.as_deref()
                    && s.trim().is_empty()
                {
                    return Err("otlp.serviceName, when set, must be non-empty".into());
                }
                if let Some(timeout) = otlp.timeout
                    && (timeout.secs_f64() <= 0.0
                        || std::time::Duration::try_from_secs_f64(timeout.secs_f64()).is_err())
                {
                    return Err("otlp.timeout, when set, must be positive and representable".into());
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn tracing_otlp_without_otlp_block_is_rejected() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: None,
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("type=Otlp requires `otlp`"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_requires_non_empty_endpoint() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "   ".into(),
                protocol: None,
                sample_ratio: None,
                service_name: None,
                timeout: None,
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.endpoint is required"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_rejects_out_of_range_sample_ratio() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel:4317".into(),
                protocol: None,
                sample_ratio: Some(1.5),
                service_name: None,
                timeout: None,
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.sampleRatio"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_rejects_zero_timeout() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel:4317".into(),
                protocol: None,
                sample_ratio: None,
                service_name: None,
                timeout: Some(Time::ZERO),
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.timeout"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_timeout_rejects_invalid_dimensioned_values() {
        for value in ["5", "1MiB", "NaNs"] {
            let json = serde_json::json!({
                "endpoint": "http://otel:4317",
                "timeout": value,
            });
            assert!(
                serde_json::from_value::<OtlpTracing>(json).is_err(),
                "accepted timeout {value}"
            );
        }

        for value in [
            "-1s",
            "999999999999999999999999999999999999999999999999999999999999s",
        ] {
            let otlp: OtlpTracing = serde_json::from_value(serde_json::json!({
                "endpoint": "http://otel:4317",
                "timeout": value,
            }))
            .expect("dimensioned timeout deserializes before semantic validation");
            let tracing = Tracing {
                kind: TracingType::Otlp,
                otlp: Some(otlp),
            };
            assert!(
                tracing.validate().is_err(),
                "accepted invalid timeout {value}"
            );
        }
    }

    #[test]
    fn tracing_otlp_with_full_spec_validates() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel-collector.observability:4317".into(),
                protocol: Some(OtlpProtocol::Grpc),
                sample_ratio: Some(0.1),
                service_name: Some("prod-cluster".into()),
                timeout: Some(Time::from_secs(5)),
            }),
        };
        assert!(t.validate().is_ok());
    }

    #[test]
    fn otlp_protocol_env_value_matches_broker_parse() {
        // The broker's `OtlpProtocol::parse` accepts "grpc" and
        // "http/protobuf" (spec values). Lock both ends.
        assert!(OtlpProtocol::Grpc.as_env_value() == "grpc");
        assert!(OtlpProtocol::HttpProtobuf.as_env_value() == "http/protobuf");
    }
}
