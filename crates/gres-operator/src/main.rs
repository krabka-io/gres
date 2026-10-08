use clap::{Parser, Subcommand};
use krabka_gres_operator::{config::OperatorConfig, gen_crds, run};

#[derive(Debug, Parser)]
#[command(name = "krabka-gres-operator", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Runs the operator. It watches the Gres CRDs and reconciles them.
    Run(Box<RunArgs>),
    /// Writes the CRD YAML manifests into a directory.
    GenCrds { out_dir: std::path::PathBuf },
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    #[command(flatten)]
    config: OperatorConfig,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // rustls 0.23 refuses to auto-pick a CryptoProvider when multiple
    // are linkable (or none is enabled at the binary level). kube's
    // rustls-tls feature pulls rustls transitively without selecting
    // one, so install ring explicitly before any TLS use.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install default rustls CryptoProvider");

    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run::run(args.config).await,
        Command::GenCrds { out_dir } => gen_crds::write_all(&out_dir),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[test]
    fn gen_crds_parses_output_directory() {
        let cli = Cli::parse_from(["bin", "gen-crds", "deploy/crds"]);
        let Command::GenCrds { out_dir } = cli.command else {
            panic!("expected GenCrds variant");
        };
        assert!(out_dir == std::path::Path::new("deploy/crds"));
    }

    #[test]
    fn run_help_lists_controller_timing_options() {
        let error = Cli::try_parse_from(["bin", "run", "--help"]).expect_err("display help");
        let help = error.to_string();
        for option in [
            "--pgdog-reload-attempts",
            "--pgdog-reload-backoff",
            "--pgdog-reload-requeue",
            "--pgdog-admin-timeout",
            "--pgdog-transition-poll",
            "--controller-error-requeue",
        ] {
            assert!(help.contains(option), "missing {option} in:\n{help}");
        }
    }
}
