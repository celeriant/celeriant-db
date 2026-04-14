use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Subset of cluster env values the chaos runner needs.
/// Other values are deliberately not parsed — the Makefile owns them.
pub struct ClusterConfig {
    pub leader_host: String,
    pub follower_host: String,
    /// None for deploy targets that don't have a separate infra node
    /// (e.g. EC2, where MinIO is replaced by real S3).
    pub infra_host: Option<String>,
    pub client_port: u16,
    pub replication_port: u16,
    pub metrics_port: u16,
    /// TCP port of the S3 endpoint. On rpi this is MinIO on the infra
    /// node (default 9000). On ec2 this would be the real S3 endpoint
    /// (443 for HTTPS), but partition scenarios targeting S3 there would
    /// need to manipulate security groups, not iptables.
    pub s3_port: u16,
    pub deploy_dir: PathBuf,
    pub ca_cert: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

impl ClusterConfig {
    /// Load config from a deploy directory. The directory must contain:
    ///
    /// - either `config.env` (rpi convention) or `.cluster-env` (ec2 CDK output)
    /// - a `Makefile` exposing the standard targets (`teardown-data`,
    ///   `start-infra`, `start-cs1`, `start-cs2`, `stop-cs1`, `stop-cs2`,
    ///   `kill-cs1`, `kill-cs2`, `stop`)
    /// - `certs/{client-ca.crt, client.crt, client.key}` for the bench mTLS client
    ///
    /// Required env keys: `LEADER_HOST`, `FOLLOWER_HOST`. Optional with
    /// defaults: `CLIENT_PORT` (10000), `METRICS_PORT` (9090), `INFRA_HOST`
    /// (None — deploys without a dedicated infra node simply omit it).
    pub fn load(deploy_dir: PathBuf) -> Result<Self, String> {
        let env_path = pick_env_file(&deploy_dir)?;
        let raw = fs::read_to_string(&env_path)
            .map_err(|e| format!("read {}: {e}", env_path.display()))?;
        let map = parse_env(&raw);

        let get_required = |k: &str| -> Result<String, String> {
            map.get(k)
                .cloned()
                .ok_or_else(|| format!("missing {k} in {}", env_path.display()))
        };

        let parse_port = |k: &str, default: u16| -> Result<u16, String> {
            match map.get(k) {
                Some(v) => v.parse().map_err(|e| format!("{k}: {e}")),
                None => Ok(default),
            }
        };

        Ok(Self {
            leader_host: get_required("LEADER_HOST")?,
            follower_host: get_required("FOLLOWER_HOST")?,
            infra_host: map.get("INFRA_HOST").filter(|s| !s.is_empty()).cloned(),
            client_port: parse_port("CLIENT_PORT", 10000)?,
            replication_port: parse_port("REPLICATION_PORT", 10001)?,
            metrics_port: parse_port("METRICS_PORT", 9090)?,
            s3_port: parse_port("S3_PORT", 9000)?,
            deploy_dir: deploy_dir.clone(),
            ca_cert: deploy_dir.join("certs/client-ca.crt"),
            client_cert: deploy_dir.join("certs/client.crt"),
            client_key: deploy_dir.join("certs/client.key"),
        })
    }

    pub fn leader_addr(&self) -> String {
        format!("{}:{}", self.leader_host, self.client_port)
    }

    pub fn follower_addr(&self) -> String {
        format!("{}:{}", self.follower_host, self.client_port)
    }

    pub fn metrics_url(&self, host: &str) -> String {
        format!("http://{}:{}/metrics", host, self.metrics_port)
    }
}

/// Picks the env file the chaos runner should read from a deploy directory.
/// Tries `config.env` (rpi convention) first, then `.cluster-env` (ec2 CDK output).
fn pick_env_file(deploy_dir: &PathBuf) -> Result<PathBuf, String> {
    for candidate in ["config.env", ".cluster-env"] {
        let p = deploy_dir.join(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    Err(format!(
        "neither config.env nor .cluster-env found in {}",
        deploy_dir.display()
    ))
}

/// Strict-enough parser for the bash-style config.env.
/// Ignores comments, blank lines, and inline shell expansion (we only need literal values).
fn parse_env(raw: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else { continue };
        let k = k.trim().to_string();
        let v = v.trim().trim_matches('"').to_string();
        out.insert(k, v);
    }
    out
}
