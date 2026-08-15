//! `dev.mcpg.cluster.etcd` — etcd `cluster` plugin.
//!
//! This crate is the implementation; operator-
//! facing summary lives in `README.md`.
//!
//! # v0.1 scope (current)
//!
//! - `node_info()` — reports operator-configured identity.
//! - `list_peers()` — etcd KV scan on `<prefix>peers/`. **NOTE:
//!   this backend does NOT self-register.** Nothing in the
//!   plugin writes a `peers/<node_id>` key, so `list_peers()` /
//!   `watch_peers()` return whatever an EXTERNAL process populates
//!   under that prefix (and empty otherwise). etcd is a
//!   coordination-only coordinator (leases/locks); peer discovery
//!   is not wired. Choose redis (or single_node) if you need
//!   gateway membership; or populate `<prefix>peers/` out-of-band.
//! - `publish(topic, routing_key, payload)` — etcd KV `put` of a
//!   timestamped key under `<prefix>events/<topic>/<ts>-<rand>`
//!   with a TTL lease (`event_ttl_secs`). The value is a tiny
//!   versioned envelope wrapping payload + routing key (see
//!   `envelope.rs`). Subscribers `Watch` the prefix and pick up
//!   the new key.
//! - `subscribe(topic, _, routing_key)` — etcd `Watch` on the
//!   topic prefix. Yields a `PublishedMessage` per `PUT` event,
//!   decoding the envelope and dropping malformed values. When
//!   the subscriber supplies a routing key, applies an exact-
//!   match filter. Watch is durable + replayable within etcd's
//!   retention window (stronger than Consul's gossip).
//! - **Lease ops** (`acquire_leadership`, `acquire_lock`,
//!   `lease_renew`, `lease_release`) via etcd's native
//!   `lease_grant` + `lock` + `lease_keep_alive` primitives.
//!   See `lease.rs` for the per-lease state machine. Background
//!   renewal task fires every `ttl × (1 - renew_pct/100)`.
//!   Fencing token is the etcd lease id (monotonic per cluster).
//! - **`watch_peers()`** — etcd `Watch` on the `peers/` prefix.
//!   PUT events emit `Joined` (or `HealthChanged` for re-registers);
//!   DELETE events emit `Left`. Stream stays open until cancelled.
//!   As with `list_peers`, this observes only EXTERNALLY-written
//!   `peers/` keys — the plugin self-registers nothing.
//!
//! # Deferred
//!
//! - **Native queue groups** via lease-locked consumer keys.
//! - **Cross-cluster federation**.

mod config;
mod envelope;
mod kv;
mod lease;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use etcd_client::{Client as EtcdClient, ConnectOptions, GetOptions, PutOptions, WatchOptions};
use mcpg_cluster_api::{
    BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend, ClusterError,
    ClusterNodeInfo, ClusterPeer, KeyValueStore, PeerHealth, PublishedMessage,
};
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::SyncClusterBackend;
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;

pub use config::{AuthConfig, ConfigError, EtcdConfig};
pub use kv::EtcdKv;

const PLUGIN_ID: &str = "dev.mcpg.cluster.etcd";

/// Shared, mutex-guarded etcd client clone-able across the coordinator's
/// own ops and the KV primitive. etcd-client takes `&mut self` on most
/// methods, so each op briefly serialises on the inner mutex.
type SharedClient = Arc<tokio::sync::Mutex<EtcdClient>>;

pub struct EtcdBackend {
    inner: Arc<Inner>,
}

struct Inner {
    manifest: PluginManifest,
    config: EtcdConfig,
    node_id: String,
    started_at: String,
    /// Lazily-established etcd client. We don't connect at boot so a
    /// broker that comes up after the gateway (or the empty-config
    /// manifest probe) doesn't panic; the cell populates on first real
    /// use. Accessors return `None` until then.
    client_cell: OnceCell<SharedClient>,
    /// Lazily-built KV primitive, populated alongside `client_cell`.
    kv_cell: OnceCell<Arc<EtcdKv>>,
    runtime: Runtime,
}

impl EtcdBackend {
    pub fn from_config_json(config_json: &str) -> Self {
        // Load-time manifest derivation builds + drops an instance only to
        // read its plugin-wide manifest. It has no real connection config, so
        // the host passes the manifest-probe sentinel (`{}`). Substitute a
        // placeholder config (lazy connect, no eager network I/O) so
        // construction succeeds for that probe; a REAL config still flows
        // through parse + validate below, so a genuinely misconfigured
        // coordinator still refuses to load.
        if mcpg_plugin_protocol::is_manifest_probe_config(config_json) {
            let cfg = EtcdConfig::parse("{\"endpoints\":[\"http://127.0.0.1:2379\"]}")
                .expect("manifest-probe placeholder etcd config is valid");
            return Self::from_validated_config(cfg);
        }
        let cfg = EtcdConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "etcd cluster: config parse failed; refusing to register"
            );
            panic!(
                "etcd cluster config parse failed: {err}. A misconfigured \
                 cluster_backend is a security hole; refusing to load."
            )
        });
        Self::from_validated_config(cfg)
    }

    fn from_validated_config(cfg: EtcdConfig) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("etcd cluster: failed to build tokio runtime");

        let node_id = cfg.resolved_node_id();
        let started_at = now_rfc3339();
        tracing::info!(
            plugin_id = PLUGIN_ID,
            endpoints = ?cfg.endpoints,
            node_id = %node_id,
            "etcd cluster: configured"
        );
        // This backend does not self-register peers — list_peers /
        // watch_peers read an externally-populated `<prefix>peers/` prefix.
        // Surface it so an operator choosing etcd for membership isn't
        // surprised by an always-empty peer set.
        tracing::warn!(
            plugin_id = PLUGIN_ID,
            peers_prefix = %cfg.peers_prefix(),
            "etcd cluster: peer self-registration is NOT implemented — \
             list_peers/watch_peers report only externally-written keys under \
             the peers prefix (empty otherwise). etcd is coordination-only \
             (leases/locks/kv); use redis or single_node for gateway membership."
        );

        let inner = Arc::new(Inner {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "etcd Cluster Coordinator".into(),
                plugin_class: PluginClass::Cluster,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                // Slot roles (cache/kv/bus), not primitive accessors.
                // etcd backs the `bus` slot via Watch-stream
                // publish/subscribe (coordinator-level) AND the `kv` slot
                // via the etcd v3 KV API (put/get/range + Txn CAS + native
                // lease TTL). It has no native cache-eviction role.
                provides: vec!["bus".into(), "kv".into()],
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            config: cfg,
            node_id,
            started_at,
            client_cell: OnceCell::new(),
            kv_cell: OnceCell::new(),
            runtime,
        });

        // Best-effort eager connect. If etcd is up at boot the cells
        // populate and the gateway's capabilities get a real
        // `key_value_store()` accessor. If etcd is down the init returns
        // `BackendUnavailable`, the cells stay empty, and the next real
        // call retries the connection — at which point they populate.
        // Accessors return `None` until then, matching the contract on
        // `ClusterBackend`.
        {
            let init_inner = Arc::clone(&inner);
            inner.runtime.block_on(async move {
                if let Err(err) = get_or_init_client(&init_inner).await {
                    tracing::warn!(
                        plugin_id = PLUGIN_ID,
                        error = %err,
                        "etcd cluster: connection unavailable at boot — primitive \
                         accessors will return None until first successful op"
                    );
                }
            });
        }

        Self { inner }
    }

    /// Resolve the shared etcd client, lazily connecting on first use.
    async fn client(&self) -> Result<SharedClient, ClusterError> {
        get_or_init_client(&self.inner).await
    }

    /// Block on the lazy connect, then resolve the live `KeyValueStore`.
    /// Returns `BackendUnavailable` when etcd is unreachable.
    fn require_kv(&self) -> Result<Arc<dyn KeyValueStore>, ClusterError> {
        self.inner
            .runtime
            .block_on(async { get_or_init_client(&self.inner).await })?;
        ClusterBackend::key_value_store(self).ok_or_else(|| ClusterError::BackendUnavailable {
            reason: "etcd cluster: key_value_store unavailable".into(),
        })
    }
}

/// Lazily connect to etcd + build the KV primitive — exactly once.
/// Subsequent calls hit the `OnceCell` fast-path. Returns
/// `BackendUnavailable` when etcd is unreachable.
async fn get_or_init_client(inner: &Arc<Inner>) -> Result<SharedClient, ClusterError> {
    let client = inner
        .client_cell
        .get_or_try_init(|| async {
            let mut connect_opts = ConnectOptions::new();
            if let Some(auth) = &inner.config.auth {
                connect_opts = connect_opts.with_user(auth.username.clone(), auth.password.clone());
            }
            // Enable TLS for https:// endpoints. The config is already
            // validated (scheme ⇄ tls consistency), so a build failure here
            // is a missing/unreadable PEM file.
            if let Some(tls) =
                inner
                    .config
                    .build_tls_options()
                    .map_err(|err| ClusterError::Internal {
                        reason: format!("etcd TLS config: {err}"),
                    })?
            {
                connect_opts = connect_opts.with_tls(tls);
            }
            let client = EtcdClient::connect(&inner.config.endpoints, Some(connect_opts))
                .await
                .map_err(|err| ClusterError::BackendUnavailable {
                    reason: format!("etcd connect: {err}"),
                })?;
            Ok::<SharedClient, ClusterError>(Arc::new(tokio::sync::Mutex::new(client)))
        })
        .await?;
    // Populate the KV primitive off the same client on first success.
    let _ = inner
        .kv_cell
        .get_or_try_init(|| async {
            Ok::<Arc<EtcdKv>, ClusterError>(Arc::new(EtcdKv::new(
                Arc::clone(client),
                format!("{}kv/", inner.config.key_prefix),
            )))
        })
        .await?;
    Ok(Arc::clone(client))
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Whole-millisecond TTL → `Duration` (None == no TTL).
fn ttl_from_ms(ttl_ms: Option<u64>) -> Option<Duration> {
    ttl_ms.map(Duration::from_millis)
}

fn now_unix_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[async_trait]
impl ClusterBackend for EtcdBackend {
    // `cluster_provides()` uses the default impl: it derives the role
    // set from `manifest().provides` (= bus, kv).

    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn key_value_store(&self) -> Option<Arc<dyn KeyValueStore>> {
        self.inner
            .kv_cell
            .get()
            .map(|kv| Arc::clone(kv) as Arc<dyn KeyValueStore>)
    }

    async fn node_info(&self) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: self.inner.node_id.clone(),
            address: self.inner.config.endpoints.join(","),
            version: env!("CARGO_PKG_VERSION").into(),
            started_at: self.inner.started_at.clone(),
            roles: vec![],
        }
    }

    async fn list_peers(&self) -> Vec<ClusterPeer> {
        let prefix = self.inner.config.peers_prefix();
        let client = match self.client().await {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = %err,
                    "etcd cluster: list_peers — backend unavailable; returning empty"
                );
                return vec![];
            }
        };
        let mut client = client.lock().await;
        let resp = match client
            .get(prefix.as_bytes(), Some(GetOptions::new().with_prefix()))
            .await
        {
            Ok(r) => r,
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = %err,
                    "etcd cluster: list_peers failed; returning empty"
                );
                return vec![];
            }
        };
        resp.kvs()
            .iter()
            .map(|kv| {
                let key = String::from_utf8_lossy(kv.key()).into_owned();
                let node_id = key
                    .strip_prefix(&prefix)
                    .map(str::to_owned)
                    .unwrap_or_else(|| key.clone());
                let address = String::from_utf8_lossy(kv.value()).into_owned();
                ClusterPeer {
                    node_id,
                    address,
                    last_seen: now_rfc3339(),
                    health: PeerHealth::Healthy,
                    roles: vec![],
                }
            })
            .collect()
    }

    async fn watch_peers(&self) -> BoxPeerEventStream {
        let prefix = self.inner.config.peers_prefix();
        let prefix_clone = prefix.clone();
        let client = match self.client().await {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = %err,
                    "etcd cluster: watch_peers — backend unavailable; returning empty stream"
                );
                return Box::pin(tokio_stream::empty());
            }
        };
        let (mut watcher, mut watch_stream) = {
            let mut c = client.lock().await;
            match c
                .watch(prefix.as_bytes(), Some(WatchOptions::new().with_prefix()))
                .await
            {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(
                        plugin_id = PLUGIN_ID,
                        error = %err,
                        "etcd cluster: watch_peers failed to start; returning empty stream"
                    );
                    return Box::pin(tokio_stream::empty());
                }
            }
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<mcpg_cluster_api::PeerEvent>(64);
        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    let _ = watcher.cancel().await;
                    break;
                }
                let resp = match watch_stream.message().await {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            error = %err,
                            "etcd cluster: watch_peers stream error; closing"
                        );
                        break;
                    }
                };
                for event in resp.events() {
                    let Some(kv) = event.kv() else { continue };
                    let key = String::from_utf8_lossy(kv.key()).into_owned();
                    let node_id = key
                        .strip_prefix(&prefix_clone)
                        .map(str::to_owned)
                        .unwrap_or_else(|| key.clone());
                    let evt = match event.event_type() {
                        etcd_client::EventType::Put => {
                            let address = String::from_utf8_lossy(kv.value()).into_owned();
                            mcpg_cluster_api::PeerEvent::Joined {
                                peer: ClusterPeer {
                                    node_id,
                                    address,
                                    last_seen: now_rfc3339(),
                                    health: PeerHealth::Healthy,
                                    roles: vec![],
                                },
                            }
                        }
                        etcd_client::EventType::Delete => {
                            mcpg_cluster_api::PeerEvent::Left { node_id }
                        }
                    };
                    if tx.send(evt).await.is_err() {
                        let _ = watcher.cancel().await;
                        return;
                    }
                }
            }
        });
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    async fn acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.key_prefix);
        let state = lease::acquire_async(
            self.client().await?,
            key,
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::EtcdLeaseHandle(state)))
    }

    async fn acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.key_prefix);
        let state = lease::acquire_async(
            self.client().await?,
            full_key,
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::EtcdLeaseHandle(state)))
    }

    async fn try_acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.key_prefix);
        let state_opt = lease::try_acquire_async(
            self.client().await?,
            key,
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::EtcdLeaseHandle(state)) as BoxActiveLease))
    }

    async fn try_acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.key_prefix);
        let state_opt = lease::try_acquire_async(
            self.client().await?,
            full_key,
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::EtcdLeaseHandle(state)) as BoxActiveLease))
    }

    async fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Bytes,
    ) -> Result<(), ClusterError> {
        // Compose a unique key per event: <topic-prefix>/<ts_ns>.
        // Subscribers Watch the topic prefix so they see every
        // PUT. The TTL lease prevents key accumulation —
        // events older than event_ttl_secs are GC'd by etcd.
        // The value is a small versioned envelope wrapping the
        // caller payload + routing key (see `envelope.rs`); etcd
        // KV values are opaque, so we have to round-trip routing
        // keys ourselves.
        let prefix = self.inner.config.topic_key_prefix(topic);
        let ts = now_unix_nanos();
        let key = format!("{prefix}{ts}");
        let wire = envelope::encode(routing_key, &payload).map_err(|e| ClusterError::Internal {
            reason: format!("publish envelope: {e}"),
        })?;
        let client = self.client().await?;
        let mut client = client.lock().await;
        let lease = client
            .lease_grant(self.inner.config.event_ttl_secs, None)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease_grant: {e}"),
            })?;
        let put_opts = PutOptions::new().with_lease(lease.id());
        client
            .put(key.into_bytes(), wire.to_vec(), Some(put_opts))
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("put: {e}"),
            })?;
        Ok(())
    }

    async fn subscribe(
        &self,
        topic: &str,
        _group: Option<&str>,
        routing_key: Option<&str>,
    ) -> Result<BoxPublishedMessageStream, ClusterError> {
        let prefix = self.inner.config.topic_key_prefix(topic);
        let topic = topic.to_owned();
        let filter_rk = routing_key.map(str::to_owned);
        let node_id = self.inner.node_id.clone();
        let client = self.client().await?;

        let (mut watcher, mut watch_stream) = {
            let mut c = client.lock().await;
            c.watch(prefix.as_bytes(), Some(WatchOptions::new().with_prefix()))
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("watch start: {e}"),
                })?
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<PublishedMessage>(64);
        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    let _ = watcher.cancel().await;
                    break;
                }
                let resp = match watch_stream.message().await {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            topic = %topic,
                            error = %err,
                            "etcd cluster: watch stream error; closing subscriber"
                        );
                        break;
                    }
                };
                for event in resp.events() {
                    // PUT events carry a kv with the value; DELETE
                    // events carry only the key (TTL expiry → DELETE
                    // — we ignore those, peers shouldn't see expired
                    // events as fresh publishes).
                    if event.event_type() != etcd_client::EventType::Put {
                        continue;
                    }
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    let raw = kv.value();
                    let (msg_rk, payload) = match envelope::decode(raw) {
                        Ok(pair) => pair,
                        Err(err) => {
                            // Drop malformed events — most likely
                            // a non-mcpg writer touched the same
                            // topic prefix.
                            tracing::warn!(
                                plugin_id = PLUGIN_ID,
                                topic = %topic,
                                error = %err,
                                "etcd cluster: dropping event with bad envelope"
                            );
                            continue;
                        }
                    };
                    if let Some(want) = filter_rk.as_deref()
                        && msg_rk.as_deref() != Some(want)
                    {
                        continue;
                    }
                    let msg = PublishedMessage {
                        topic: topic.clone(),
                        routing_key: msg_rk,
                        payload,
                        from_node: node_id.clone(),
                    };
                    if tx.send(msg).await.is_err() {
                        let _ = watcher.cancel().await;
                        return;
                    }
                }
            }
        });
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

impl SyncClusterBackend for EtcdBackend {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn node_info(&self) -> ClusterNodeInfo {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::node_info(self).await })
    }

    fn list_peers(&self) -> Vec<ClusterPeer> {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::list_peers(self).await })
    }

    fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<(), ClusterError> {
        self.inner.runtime.block_on(async {
            ClusterBackend::publish(self, topic, routing_key, Bytes::from(payload)).await
        })
    }

    // Bridge the async pub/sub + peer-watch impls across the FFI via
    // the shared `cluster_forward` helper.
    fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<mcpg_plugin_sdk::ffi::WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::subscribe(self, topic, group, routing_key).await })?;
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn watch_peers(
        &self,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<mcpg_plugin_sdk::ffi::WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::watch_peers(self).await });
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn cancel_stream(&self, stream_handle: mcpg_plugin_sdk::ffi::WatchHandleBox) {
        // SAFETY: handle came from our subscribe/watch_peers, not yet cancelled.
        unsafe { mcpg_plugin_sdk::ffi::cluster_forward::cancel_cluster_stream(stream_handle) }
    }

    fn acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<(mcpg_plugin_sdk::ffi::WatchHandleBox, u64, String), ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.key_prefix);
        let client = self
            .inner
            .runtime
            .block_on(async { get_or_init_client(&self.inner).await })?;
        lease::acquire_sync(
            self.inner.runtime.handle(),
            client,
            key,
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<(mcpg_plugin_sdk::ffi::WatchHandleBox, u64, String), ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.key_prefix);
        let client = self
            .inner
            .runtime
            .block_on(async { get_or_init_client(&self.inner).await })?;
        lease::acquire_sync(
            self.inner.runtime.handle(),
            client,
            full_key,
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<Option<(mcpg_plugin_sdk::ffi::WatchHandleBox, u64, String)>, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.key_prefix);
        let client = self
            .inner
            .runtime
            .block_on(async { get_or_init_client(&self.inner).await })?;
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            client,
            key,
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<Option<(mcpg_plugin_sdk::ffi::WatchHandleBox, u64, String)>, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.key_prefix);
        let client = self
            .inner
            .runtime
            .block_on(async { get_or_init_client(&self.inner).await })?;
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            client,
            full_key,
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn lease_renew(
        &self,
        lease_handle: mcpg_plugin_sdk::ffi::WatchHandleBox,
    ) -> Result<String, ClusterError> {
        lease::renew_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_release(
        &self,
        lease_handle: mcpg_plugin_sdk::ffi::WatchHandleBox,
    ) -> Result<(), ClusterError> {
        lease::release_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_drop(&self, lease_handle: mcpg_plugin_sdk::ffi::WatchHandleBox) {
        // SAFETY: host vtable contract — exactly one `lease_drop`
        // per acquire, and the pointer is still valid.
        unsafe { lease::drop_state(lease_handle) }
    }

    // KV primitive over FFI — block on the plugin's own runtime, routing
    // each method through the same `KeyValueStore` impl `key_value_store()`
    // exposes.
    fn kv_get(&self, key: &str) -> Result<Option<mcpg_cluster_api::Entry>, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async { kv.get(key).await })
    }

    fn kv_put(&self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Result<(), ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.put(key, Bytes::from(value), ttl_from_ms(ttl_ms)).await })
    }

    fn kv_put_if_absent(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async {
            kv.put_if_absent(key, Bytes::from(value), ttl_from_ms(ttl_ms))
                .await
        })
    }

    fn kv_delete(&self, key: &str) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner.runtime.block_on(async { kv.delete(key).await })
    }

    fn kv_list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, mcpg_cluster_api::Entry)>, ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.list_prefix(prefix, limit).await })
    }

    fn kv_expire(&self, key: &str, ttl_ms: Option<u64>) -> Result<bool, ClusterError> {
        let kv = self.require_kv()?;
        self.inner
            .runtime
            .block_on(async { kv.expire(key, ttl_from_ms(ttl_ms)).await })
    }

    /// etcd holds no backend-level background task. Its only spawned
    /// tasks are per-stream (`subscribe` / `watch_peers`, torn down by the
    /// host via `cancel_stream`) and per-lease keep-alive (owned by each
    /// `ActiveLease`, torn down via `lease_release` / `lease_drop`) — all
    /// drained through their own vtable slots within the host's window. So
    /// `shutdown` has nothing of its own to abort; it just records the drain.
    fn shutdown(&self) {
        tracing::info!(
            plugin_id = PLUGIN_ID,
            "etcd cluster: shutdown — no backend-level background tasks \
             (streams/leases drain via their own handles)"
        );
    }
}

declare_plugin! {
    plugin_id: "dev.mcpg.cluster.etcd",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        cluster_backend as cluster {
            inner_name: "",
            plugin_type: EtcdBackend,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| -> EtcdBackend {
                EtcdBackend::from_config_json(cfg)
            },
        }
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_validation_works() {
        let cfg = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "node_id": "node-test"
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(cfg.resolved_node_id(), "node-test");
        assert_eq!(cfg.peer_key("alpha"), "/mcpg/peers/alpha");
        assert_eq!(cfg.topic_key_prefix("x.y"), "/mcpg/events/x.y/");
    }

    #[test]
    fn config_resolved_node_id_falls_back_to_synthetic() {
        let cfg =
            EtcdConfig::parse(&json!({ "endpoints": ["http://etcd:2379"] }).to_string()).unwrap();
        assert!(cfg.resolved_node_id().starts_with("/mcpg/node-"));
    }

    #[test]
    fn auth_config_roundtrips() {
        // Auth requires an https:// endpoint + a tls block
        // (auth over http:// is rejected to keep the password off the wire).
        let cfg = EtcdConfig::parse(
            &json!({
                "endpoints": ["https://etcd:2379"],
                "tls": {},
                "auth": { "username": "alice", "password": "secret" }
            })
            .to_string(),
        )
        .unwrap();
        let auth = cfg.auth.as_ref().unwrap();
        assert_eq!(auth.username, "alice");
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn key_prefix_namespacing_isolates_deployments() {
        let prod = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "key_prefix": "/mcpg-prod/"
            })
            .to_string(),
        )
        .unwrap();
        let staging = EtcdConfig::parse(
            &json!({
                "endpoints": ["http://etcd:2379"],
                "key_prefix": "/mcpg-staging/"
            })
            .to_string(),
        )
        .unwrap();
        assert_ne!(
            prod.topic_key_prefix("creds.events"),
            staging.topic_key_prefix("creds.events")
        );
    }

    // Live etcd integration tests are out of scope for v0.1 —
    // they need testcontainers and a running etcd instance. The
    // plugin's pre-connection logic (config validation, key
    // helpers, node_id resolution) is unit-tested here; the
    // gRPC paths (publish, subscribe, list_peers) are validated
    // at gateway-level integration tests against a real etcd
    // backend.
}
