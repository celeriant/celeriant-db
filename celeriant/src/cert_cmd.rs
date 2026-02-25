use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

use celeriant_crypto::pki::{PkiError, PkiManager};
use clap::{Parser, Subcommand};
use x509_parser::extensions::GeneralName;

/// Top-level `celeriant cert` command.
#[derive(Debug, Parser)]
#[command(name = "celeriant cert", about = "Certificate lifecycle management")]
struct CertArgs {
    #[command(subcommand)]
    command: CertCommand,
}

#[derive(Debug, Subcommand)]
enum CertCommand {
    /// Generate a self-signed cluster CA certificate.
    CreateCa {
        /// Directory where ca.crt and ca.key will be written.
        #[arg(long)]
        ca_dir: PathBuf,

        /// Certificate validity in days.
        #[arg(long, default_value_t = 3650)]
        validity_days: u32,
    },

    /// Generate a node certificate signed by the cluster CA.
    CreateNode {
        /// Hostnames and/or IP addresses for Subject Alternative Names (positional).
        #[arg(required = true)]
        hosts: Vec<String>,

        /// Directory containing ca.crt and ca.key.
        #[arg(long)]
        ca_dir: PathBuf,

        /// Directory where node.crt and node.key will be written.
        #[arg(long)]
        cert_dir: PathBuf,

        /// Certificate validity in days.
        #[arg(long, default_value_t = 90)]
        validity_days: u32,
    },

    /// Generate a client certificate signed by the cluster CA.
    CreateClient {
        /// Client name used in the certificate CN and output filenames.
        client_name: String,

        /// Directory containing ca.crt and ca.key.
        #[arg(long)]
        ca_dir: PathBuf,

        /// Directory where client-<name>.crt and client-<name>.key will be written.
        #[arg(long)]
        cert_dir: PathBuf,

        /// Certificate validity in days.
        #[arg(long, default_value_t = 90)]
        validity_days: u32,
    },

    /// List and inspect certificates in a directory.
    List {
        /// Directory containing .crt files to inspect.
        #[arg(long)]
        cert_dir: PathBuf,
    },
}

/// Run a `cert` subcommand parsed from argv (everything after "cert").
pub fn run_cert(argv: Vec<String>) -> Result<(), PkiError> {
    // Prepend the binary name so clap can parse correctly.
    let mut args = vec!["celeriant-cert".to_string()];
    args.extend(argv);

    let cert_args = CertArgs::parse_from(args);
    match cert_args.command {
        CertCommand::CreateCa { ca_dir, validity_days } => {
            PkiManager::create_ca(&ca_dir, validity_days)?;
            println!("CA certificate written to {}", ca_dir.display());
            println!("  {}/ca.crt", ca_dir.display());
            println!("  {}/ca.key  (keep secret, permissions 0600)", ca_dir.display());
        }

        CertCommand::CreateNode { hosts, ca_dir, cert_dir, validity_days } => {
            PkiManager::create_node_cert(&ca_dir, &cert_dir, &hosts, validity_days)?;
            println!("Node certificate written to {}", cert_dir.display());
            println!("  {}/node.crt", cert_dir.display());
            println!("  {}/node.key  (keep secret, permissions 0600)", cert_dir.display());
        }

        CertCommand::CreateClient { client_name, ca_dir, cert_dir, validity_days } => {
            PkiManager::create_client_cert(&ca_dir, &cert_dir, &client_name, validity_days)?;
            println!("Client certificate written to {}", cert_dir.display());
            println!("  {}/client-{client_name}.crt", cert_dir.display());
            println!("  {}/client-{client_name}.key  (keep secret, permissions 0600)", cert_dir.display());
        }

        CertCommand::List { cert_dir } => list_certs(&cert_dir)?,
    }

    Ok(())
}

fn list_certs(cert_dir: &std::path::Path) -> Result<(), PkiError> {
    let mut entries: Vec<_> = fs::read_dir(cert_dir)?
        .filter_map(|e| e.map_err(|err| eprintln!("Warning: directory entry error: {err}")).ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x == "crt")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    if entries.is_empty() {
        println!("No .crt files found in {}", cert_dir.display());
        return Ok(());
    }

    for entry in entries {
        print_cert_info(&entry.path());
    }

    Ok(())
}

fn print_cert_info(path: &std::path::Path) {
    let certs = match PkiManager::load_ca_bundle(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}: load error: {e:?}", path.display());
            return;
        }
    };

    let (_, cert) = match x509_parser::parse_x509_certificate(certs[0].as_ref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}: parse error: {e}", path.display());
            return;
        }
    };

    println!("{}", path.display());
    println!("  Subject: {}", cert.subject());
    println!("  Issuer:  {}", cert.issuer());

    let not_before = cert.validity().not_before.to_datetime();
    let not_after = cert.validity().not_after.to_datetime();
    println!("  Valid:   {} → {}", not_before, not_after);

    if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
        let names: Vec<String> = san_ext
            .value
            .general_names
            .iter()
            .map(format_san)
            .collect();
        if !names.is_empty() {
            println!("  SANs:    {}", names.join(", "));
        }
    }

    println!();
}

fn format_san(n: &GeneralName<'_>) -> String {
    match n {
        GeneralName::DNSName(s) => s.to_string(),
        GeneralName::IPAddress(b) => match b.len() {
            4 => IpAddr::V4(Ipv4Addr::new(b[0], b[1], b[2], b[3])).to_string(),
            16 => b[..16]
                .try_into()
                .map(|octets: [u8; 16]| IpAddr::V6(Ipv6Addr::from(octets)).to_string())
                .unwrap_or_else(|_| format!("{n:?}")),
            _ => format!("{n:?}"),
        },
        _ => format!("{n:?}"),
    }
}
