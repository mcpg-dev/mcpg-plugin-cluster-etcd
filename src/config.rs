//! Operator-supplied configuration schema for `dev.mcpg.cluster.etcd`.

use etcd_client::{Certificate, Identity, TlsOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Resolve a PEM value that is either inline (starts with `-----BEGIN`)
/// or a filesystem path, into raw bytes for `Certificate`/`Identity`.
fn resolve_pem(value: &str) -> Result<Vec<u8>, ConfigError> {
    if value.trim_start().starts_with("-----BEGIN") {
        Ok(value.as_bytes().to_vec())
    } else {
        std::fs::read(value).map_err(|source| ConfigError::TlsFileRead {
            path: value.to_owned(),
            source,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdConfig {
    /// One or more etcd endpoints — e.g.
    /// `["http://etcd-0:2379", "http://etcd-1:2379"]`.
    /// etcd-client load-balances across them; if one is down it
    /// retries against the others.
    pub endpoints: Vec<String>,

    /// Optional username/password for etcd Auth-enabled clusters.
    /// REJECTED over a plaintext (`http://`) endpoint — the password
    /// must not cross the wire in cleartext (fail-closed).
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Optional TLS for `https://` endpoints. Required when any
    /// endpoint is `https://`; rejected when all endpoints are `http://`.
    /// Server-cert verification is always on (etcd-client/tonic exposes
    /// no skip-verify). `ca_cert` adds a private-CA root (else the system
    /// roots are used); `client_cert`+`client_key` enable mTLS.
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// Key prefix the plugin uses for its data. Operators
    /// running multiple MCPG deployments on one etcd cluster
    /// MUST set distinct prefixes per deployment to avoid
    /// cross-talk. Default `/mcpg/`.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,

    /// This node's stable identifier. Defaults to a synthetic
    /// "{key_prefix}-{hostname}" value.
    #[serde(default)]
    pub node_id: Option<String>,

    /// TTL for transient pub/sub events (seconds). Events expire
    /// from etcd after this many seconds — operators tune for
    /// "how long can a slow subscriber afford to be behind."
    /// Default 60s.
    #[serde(default = "default_event_ttl_secs")]
    pub event_ttl_secs: i64,

    /// Background renewal task fires every
    /// `ttl × (100 - pct) / 100`. Default 30 — renewal at 70% of
    /// TTL, leaving 30% margin for the keep-alive RTT + scheduling
    /// jitter. Clamped to `[1, 99]` at runtime.
    #[serde(default = "default_renew_pct")]
    pub lease_renew_before_expiry_percent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
}

/// TLS knobs for `https://` etcd endpoints. PEM values are either
/// inline (a string starting with `-----BEGIN`) or a filesystem path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Private-CA root certificate (PEM, inline or path). When absent,
    /// the system trust roots are used. Server-cert verification is
    /// always on.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Client certificate (PEM, inline or path) for mTLS. Must be set
    /// together with `client_key`.
    #[serde(default)]
    pub client_cert: Option<String>,
    /// Client private key (PEM, inline or path) for mTLS.
    #[serde(default)]
    pub client_key: Option<String>,
    /// Optional SNI / certificate domain override (defaults to the
    /// endpoint host).
    #[serde(default)]
    pub domain_name: Option<String>,
}

fn default_key_prefix() -> String {
    "/mcpg/".into()
}

fn default_event_ttl_secs() -> i64 {
    60
}

fn default_renew_pct() -> u32 {
    30
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid cluster.etcd config JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("cluster.etcd: endpoints must be non-empty")]
    EmptyEndpoints,
    #[error("cluster.etcd: endpoint[{index}] is empty")]
    EmptyEndpoint { index: usize },
    #[error(
        "cluster.etcd: endpoint[{index}] (`{endpoint}`) has no scheme — etcd-client connects a \
         scheme-less `host:port` in plaintext. Use an explicit `https://` (or `http://`) scheme"
    )]
    MissingScheme { index: usize, endpoint: String },
    #[error("cluster.etcd: key_prefix must end with '/'")]
    KeyPrefixMissingTrailingSlash,
    #[error("cluster.etcd: event_ttl_secs must be > 0")]
    InvalidEventTtl,
    #[error(
        "cluster.etcd: an https:// endpoint requires a `tls` block (fail-closed) — \
         add `tls: {{}}` to use system roots, or `tls: {{ ca_cert: ... }}` for a private CA"
    )]
    HttpsRequiresTls,
    #[error(
        "cluster.etcd: `auth` over a plaintext http:// endpoint would send the password in \
         cleartext — use https:// (with a `tls` block) for an auth-enabled cluster"
    )]
    PlaintextAuth,
    #[error(
        "cluster.etcd: a `tls` block is set but no endpoint is https:// — drop `tls` or use https://"
    )]
    TlsWithoutHttps,
    #[error("cluster.etcd: endpoints mix http:// and https:// — use one scheme for all endpoints")]
    MixedSchemes,
    #[error("cluster.etcd: tls.client_cert and tls.client_key must be set together (mTLS)")]
    MtlsHalfPair,
    #[error("cluster.etcd: could not read TLS PEM file `{path}`: {source}")]
    TlsFileRead {
        path: String,
        source: std::io::Error,
    },
}

impl EtcdConfig {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.endpoints.is_empty() {
            return Err(ConfigError::EmptyEndpoints);
        }
        for (index, ep) in self.endpoints.iter().enumerate() {
            let trimmed = ep.trim();
            if trimmed.is_empty() {
                return Err(ConfigError::EmptyEndpoint { index });
            }
            // Require an explicit scheme: etcd-client connects a scheme-less
            // `host:port` over plaintext HTTP, which would silently dodge the
            // https/tls fail-closed checks below (and the gateway's guard).
            if !trimmed.starts_with("https://") && !trimmed.starts_with("http://") {
                return Err(ConfigError::MissingScheme {
                    index,
                    endpoint: ep.clone(),
                });
            }
        }
        if !self.key_prefix.ends_with('/') {
            return Err(ConfigError::KeyPrefixMissingTrailingSlash);
        }
        if self.event_ttl_secs <= 0 {
            return Err(ConfigError::InvalidEventTtl);
        }
        // Transport security (fail-closed). Classify endpoint schemes.
        let any_https = self
            .endpoints
            .iter()
            .any(|e| e.trim().starts_with("https://"));
        let any_http = self
            .endpoints
            .iter()
            .any(|e| e.trim().starts_with("http://"));
        if any_http && any_https {
            return Err(ConfigError::MixedSchemes);
        }
        if any_https && self.tls.is_none() {
            return Err(ConfigError::HttpsRequiresTls);
        }
        if self.tls.is_some() && !any_https {
            return Err(ConfigError::TlsWithoutHttps);
        }
        if self.auth.is_some() && any_http {
            return Err(ConfigError::PlaintextAuth);
        }
        if let Some(tls) = &self.tls
            && tls.client_cert.is_some() != tls.client_key.is_some()
        {
            return Err(ConfigError::MtlsHalfPair);
        }
        Ok(())
    }

    /// Build the etcd-client TLS options from the validated config.
    /// `Ok(None)` when no `tls` block is set (plaintext http path).
    /// Reads PEM material (inline or file) here, at connect time.
    pub(crate) fn build_tls_options(&self) -> Result<Option<TlsOptions>, ConfigError> {
        let Some(tls) = &self.tls else {
            return Ok(None);
        };
        let mut opts = TlsOptions::new();
        match &tls.ca_cert {
            Some(ca) => opts = opts.ca_certificate(Certificate::from_pem(resolve_pem(ca)?)),
            // No private-CA root → verify against the system trust store.
            None => opts = opts.with_native_roots(),
        }
        if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
            opts = opts.identity(Identity::from_pem(resolve_pem(cert)?, resolve_pem(key)?));
        }
        if let Some(domain) = &tls.domain_name {
            opts = opts.domain_name(domain.clone());
        }
        Ok(Some(opts))
    }

    pub fn resolved_node_id(&self) -> String {
        self.node_id.clone().unwrap_or_else(|| {
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
            format!("{}node-{host}", self.key_prefix)
        })
    }

    /// Per-instance key for self-registration in
    /// `<prefix>peers/<node_id>`.
    pub fn peer_key(&self, node_id: &str) -> String {
        format!("{}peers/{node_id}", self.key_prefix)
    }

    pub fn peers_prefix(&self) -> String {
        format!("{}peers/", self.key_prefix)
    }

    pub fn topic_key_prefix(&self, topic: &str) -> String {
        format!("{}events/{topic}/", self.key_prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_minimal_config() {
        let cfg = json!({
            "endpoints": ["http://etcd:2379"]
        })
        .to_string();
        let parsed = EtcdConfig::parse(&cfg).unwrap();
        assert_eq!(parsed.key_prefix, "/mcpg/");
        assert_eq!(parsed.event_ttl_secs, 60);
    }

    #[test]
    fn rejects_empty_endpoints() {
        let cfg = json!({ "endpoints": [] }).to_string();
        let err = EtcdConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::EmptyEndpoints);
    }

    #[test]
    fn rejects_prefix_without_trailing_slash() {
        let cfg = json!({
            "endpoints": ["http://etcd:2379"],
            "key_prefix": "/mcpg"
        })
        .to_string();
        let err = EtcdConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::KeyPrefixMissingTrailingSlash);
    }

    #[test]
    fn rejects_zero_event_ttl() {
        let cfg = json!({
            "endpoints": ["http://etcd:2379"],
            "event_ttl_secs": 0
        })
        .to_string();
        let err = EtcdConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::InvalidEventTtl);
    }

    #[test]
    fn rejects_https_endpoint_without_tls() {
        let err = EtcdConfig::parse(&json!({"endpoints": ["https://etcd:2379"]}).to_string())
            .unwrap_err();
        assert!(matches!(err, ConfigError::HttpsRequiresTls), "{err}");
    }

    #[test]
    fn rejects_auth_over_plaintext_http() {
        let err = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "auth": {"username": "u", "password": "p"}
            })
            .to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::PlaintextAuth), "{err}");
    }

    #[test]
    fn accepts_https_with_tls_system_roots() {
        let cfg =
            EtcdConfig::parse(&json!({"endpoints": ["https://etcd:2379"], "tls": {}}).to_string())
                .unwrap();
        assert!(cfg.tls.is_some());
        assert!(cfg.build_tls_options().unwrap().is_some());
    }

    #[test]
    fn accepts_https_with_inline_ca_pem() {
        // An inline PEM (not a path) must not be treated as a file read.
        let cfg = EtcdConfig::parse(
            &json!({
                "endpoints": ["https://etcd:2379"],
                "tls": {"ca_cert": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}
            })
            .to_string(),
        )
        .unwrap();
        assert!(cfg.build_tls_options().unwrap().is_some());
    }

    #[test]
    fn rejects_scheme_less_endpoint() {
        // A bare `host:port` endpoint connects plaintext in etcd-client, so
        // the plugin must reject it rather than dodge the fail-closed checks.
        let err =
            EtcdConfig::parse(&json!({"endpoints": ["etcd-0:2379"]}).to_string()).unwrap_err();
        assert!(matches!(err, ConfigError::MissingScheme { .. }), "{err}");
        // A scheme-less endpoint mixed with a valid https one is still caught.
        let err2 = EtcdConfig::parse(
            &json!({"endpoints": ["https://etcd-0:2379", "etcd-1:2379"], "tls": {}}).to_string(),
        )
        .unwrap_err();
        assert!(matches!(err2, ConfigError::MissingScheme { .. }), "{err2}");
    }

    #[test]
    fn rejects_tls_block_with_only_http_endpoints() {
        let err =
            EtcdConfig::parse(&json!({"endpoints": ["http://etcd:2379"], "tls": {}}).to_string())
                .unwrap_err();
        assert!(matches!(err, ConfigError::TlsWithoutHttps), "{err}");
    }

    #[test]
    fn rejects_mixed_http_and_https_endpoints() {
        let err = EtcdConfig::parse(
            &json!({"endpoints": ["http://a:2379", "https://b:2379"], "tls": {}}).to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::MixedSchemes), "{err}");
    }

    #[test]
    fn rejects_mtls_client_cert_without_key() {
        let err = EtcdConfig::parse(
            &json!({
                "endpoints": ["https://etcd:2379"],
                "tls": {"client_cert": "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----"}
            })
            .to_string(),
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::MtlsHalfPair), "{err}");
    }

    #[test]
    fn build_tls_options_none_when_no_tls_block() {
        // Plaintext http (no auth) stays valid for dev and yields no TLS.
        let cfg =
            EtcdConfig::parse(&json!({"endpoints": ["http://etcd:2379"]}).to_string()).unwrap();
        assert!(cfg.build_tls_options().unwrap().is_none());
    }

    #[test]
    fn lease_renew_pct_defaults_to_30() {
        let cfg =
            EtcdConfig::parse(&json!({"endpoints": ["http://etcd:2379"]}).to_string()).unwrap();
        assert_eq!(cfg.lease_renew_before_expiry_percent, 30);
    }

    #[test]
    fn lease_renew_pct_overridable() {
        let cfg = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "lease_renew_before_expiry_percent": 50
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(cfg.lease_renew_before_expiry_percent, 50);
    }

    #[test]
    fn key_helpers_use_prefix() {
        let cfg = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "key_prefix": "/mcpg-prod/"
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(cfg.peer_key("alpha"), "/mcpg-prod/peers/alpha");
        assert_eq!(cfg.peers_prefix(), "/mcpg-prod/peers/");
        assert_eq!(
            cfg.topic_key_prefix("creds.events"),
            "/mcpg-prod/events/creds.events/"
        );
    }
}
