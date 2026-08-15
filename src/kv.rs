//! etcd-backed `KeyValueStore` for `dev.mcpg.cluster.etcd`.
//!
//! Maps the host's durable KV primitive onto the etcd v3 KV API:
//!
//! | Trait method     | etcd primitive |
//! |---|---|
//! | `get`            | `get` (single key); `lease_time_to_live` for `expires_at` |
//! | `put`            | `lease_grant` + `put(.. with_lease)` (TTL), or plain `put` (no TTL) |
//! | `put_if_absent`  | `Txn` comparing `create_revision == 0` → put (single-winner) |
//! | `delete`         | `delete(.. with_prev_key)` → `deleted()` count |
//! | `list_prefix`    | `get(.. with_prefix().with_limit())` |
//! | `expire`         | re-`put` the existing value under a fresh lease (or none) |
//!
//! # TTL — native etcd leases
//!
//! etcd has first-class lease TTLs: a key with an attached lease is
//! deleted by etcd when the lease expires. Every `put`/`put_if_absent`
//! with a `ttl` grants a fresh lease (whole-second granularity — etcd
//! lease TTLs are seconds, so a sub-second `ttl` rounds up to 1 s) and
//! attaches it; a TTL-less write uses a plain put (which detaches any
//! prior lease, leaving the key permanent). On `get`/`list_prefix`,
//! `expires_at` is reconstructed from the key's attached lease via
//! `lease_time_to_live`. "Expired == absent" therefore holds natively:
//! etcd removes the key when its lease lapses.
//!
//! # Versioning / CAS
//!
//! `put_if_absent` is the cross-replica single-winner claim. It is a
//! single etcd transaction comparing the target key's `create_revision`
//! against 0 (absent) and, only when that holds, putting the value —
//! atomic against the backing store, so no two concurrent callers can
//! both observe `true`. The trait's `Entry` does not surface a version
//! field, so per-key `mod_revision`/`create_revision` stay internal to
//! this backend (used only for the CAS compare).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use etcd_client::{Client as EtcdClient, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore};

/// etcd-backed KV state. Shares the coordinator's single client behind
/// the same tokio `Mutex` (etcd-client takes `&mut self` on every op).
pub struct EtcdKv {
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    key_prefix: String,
}

impl std::fmt::Debug for EtcdKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdKv")
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl EtcdKv {
    /// Construct an `EtcdKv` over the coordinator's shared client. The
    /// `kv_prefix` namespaces KV keys away from the coordinator's
    /// `leadership/`, `locks/`, `events/`, and `peers/` keyspaces.
    pub fn new(client: Arc<tokio::sync::Mutex<EtcdClient>>, kv_prefix: String) -> Self {
        Self {
            client,
            key_prefix: kv_prefix,
        }
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix)
    }

    /// Strip the impl-prepended prefix from a key etcd returned, so the
    /// caller sees the logical key it `put`.
    fn logical_key(&self, full: &str) -> String {
        full.strip_prefix(&self.key_prefix)
            .map(str::to_owned)
            .unwrap_or_else(|| full.to_owned())
    }
}

/// Convert a `ttl` into a whole-second etcd lease TTL. etcd lease TTLs
/// are seconds; a sub-second TTL rounds up to 1 s so a positive TTL
/// never collapses to "no expiry".
fn lease_secs(ttl: Duration) -> i64 {
    let secs = ttl.as_secs();
    if secs == 0 {
        1
    } else {
        secs.min(i64::MAX as u64) as i64
    }
}

fn unavailable(op: &str, e: impl std::fmt::Display) -> ClusterError {
    ClusterError::BackendUnavailable {
        reason: format!("etcd kv {op}: {e}"),
    }
}

impl EtcdKv {
    /// Compute `expires_at` for a key with an attached lease id. A lease
    /// id of 0 means no lease → no expiry. A `lease_time_to_live` of
    /// `<= 0` means the lease has already lapsed (key on its way out).
    async fn expires_at_for_lease(
        &self,
        client: &mut EtcdClient,
        lease_id: i64,
    ) -> Option<SystemTime> {
        if lease_id == 0 {
            return None;
        }
        match client.lease_time_to_live(lease_id, None).await {
            Ok(resp) if resp.ttl() > 0 => {
                Some(SystemTime::now() + Duration::from_secs(resp.ttl() as u64))
            }
            _ => None,
        }
    }
}

#[async_trait]
impl KeyValueStore for EtcdKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        let full = self.full_key(key);
        let mut client = self.client.lock().await;
        let resp = client
            .get(full.into_bytes(), None)
            .await
            .map_err(|e| unavailable("get", e))?;
        let Some(kv) = resp.kvs().first() else {
            return Ok(None);
        };
        let bytes = Bytes::copy_from_slice(kv.value());
        let lease_id = kv.lease();
        let expires_at = self.expires_at_for_lease(&mut client, lease_id).await;
        Ok(Some(Entry { bytes, expires_at }))
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), ClusterError> {
        let full = self.full_key(key);
        let mut client = self.client.lock().await;
        match ttl {
            Some(d) => {
                let lease = client
                    .lease_grant(lease_secs(d), None)
                    .await
                    .map_err(|e| unavailable("lease_grant", e))?;
                let opts = PutOptions::new().with_lease(lease.id());
                client
                    .put(full.into_bytes(), value.to_vec(), Some(opts))
                    .await
                    .map_err(|e| unavailable("put", e))?;
            }
            None => {
                // A plain put replaces the value and detaches any prior
                // lease, leaving the key permanent.
                client
                    .put(full.into_bytes(), value.to_vec(), None)
                    .await
                    .map_err(|e| unavailable("put", e))?;
            }
        }
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut client = self.client.lock().await;
        // Grant the lease BEFORE the txn so the put can attach it
        // atomically. If the txn loses the race, revoke the unused lease
        // so etcd doesn't carry it for the full TTL.
        let lease_id = match ttl {
            Some(d) => Some(
                client
                    .lease_grant(lease_secs(d), None)
                    .await
                    .map_err(|e| unavailable("lease_grant", e))?
                    .id(),
            ),
            None => None,
        };
        let put_opts = lease_id.map(|id| PutOptions::new().with_lease(id));
        // Single-winner claim: put only when the key has never been
        // created (create_revision == 0 ⇔ absent; an expired key has
        // already been deleted by etcd, so it counts as absent too).
        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                full.as_str(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(full.as_str(), value.to_vec(), put_opts)])
            .or_else(vec![]);
        let resp = client.txn(txn).await.map_err(|e| unavailable("txn", e))?;
        if !resp.succeeded() {
            if let Some(id) = lease_id {
                let _ = client.lease_revoke(id).await;
            }
            return Ok(false);
        }
        Ok(true)
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut client = self.client.lock().await;
        let resp = client
            .delete(full.into_bytes(), None)
            .await
            .map_err(|e| unavailable("delete", e))?;
        Ok(resp.deleted() > 0)
    }

    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        let full_prefix = self.full_key(prefix);
        let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
        let opts = GetOptions::new().with_prefix().with_limit(limit_i64);
        let mut client = self.client.lock().await;
        let resp = client
            .get(full_prefix.into_bytes(), Some(opts))
            .await
            .map_err(|e| unavailable("list_prefix", e))?;
        // Collect (logical_key, value, lease_id) first so the per-key
        // lease_time_to_live calls don't borrow `resp` across the await.
        let raw: Vec<(String, Bytes, i64)> = resp
            .kvs()
            .iter()
            .map(|kv| {
                let full = String::from_utf8_lossy(kv.key()).into_owned();
                (
                    self.logical_key(&full),
                    Bytes::copy_from_slice(kv.value()),
                    kv.lease(),
                )
            })
            .collect();
        let mut out = Vec::with_capacity(raw.len());
        for (logical, bytes, lease_id) in raw {
            let expires_at = self.expires_at_for_lease(&mut client, lease_id).await;
            out.push((logical, Entry { bytes, expires_at }));
        }
        Ok(out)
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let mut client = self.client.lock().await;
        // Read the current value (TTL changes must preserve it). Absent
        // → false per the contract.
        let resp = client
            .get(full.clone().into_bytes(), None)
            .await
            .map_err(|e| unavailable("expire get", e))?;
        let Some(kv) = resp.kvs().first() else {
            return Ok(false);
        };
        let value = kv.value().to_vec();
        match ttl {
            Some(d) => {
                let lease = client
                    .lease_grant(lease_secs(d), None)
                    .await
                    .map_err(|e| unavailable("expire lease_grant", e))?;
                let opts = PutOptions::new().with_lease(lease.id());
                client
                    .put(full.into_bytes(), value, Some(opts))
                    .await
                    .map_err(|e| unavailable("expire put", e))?;
            }
            None => {
                // Drop the TTL but keep the value — a plain put detaches
                // the prior lease.
                client
                    .put(full.into_bytes(), value, None)
                    .await
                    .map_err(|e| unavailable("expire put", e))?;
            }
        }
        Ok(true)
    }
}
