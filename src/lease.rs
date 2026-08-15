//! Lease lifecycle for `dev.mcpg.cluster.etcd`.
//!
//! Etcd has a native lock + lease story — `lease_grant` returns
//! a lease id with a TTL, `lock(name, lease_id)` atomically takes
//! a distributed lock keyed to that lease, and `lease_keep_alive`
//! refreshes the TTL. We bind those primitives 1:1 to the
//! `ActiveLease` trait surface:
//!
//! - `acquire_leadership(role, ttl)` and `acquire_lock(key, ttl)`
//!   use `lock("/<prefix>{leadership|locks}/<name>", lease_id)`.
//!   Both block until they hold the lock — leadership semantics
//!   match this; the operator who wants non-blocking lock
//!   acquisition asks for a short lease_ttl and falls back to
//!   `Timeout`.
//! - **Fencing token** is the etcd lease id. Etcd lease ids are
//!   monotonic per cluster lifetime (`Lease ID is the lease ID
//!   of the granted lease` — issued from a global counter on the
//!   leader). They wrap to `i64::MAX` so casting to u64 is safe.
//! - **Renewal** is `lease_keep_alive` — etcd's keep-alive is a
//!   bidirectional stream; we use the simpler one-shot
//!   `lease_keep_alive` call from etcd-client + a background task
//!   that fires every `ttl × (1 - renew_before_expiry_percent /
//!   100)`. Plugins that want explicit control invoke
//!   `lease_renew` themselves.
//! - **Release** is `unlock(lock_key) + lease_revoke(lease_id)`.
//!   Idempotent via an `AtomicBool`. The background renewal task
//!   aborts on drop.
//!
//! # State lifecycle
//!
//! `acquire_*` produces an `Arc<LeaseState>`. The async path
//! wraps it in `EtcdLeaseHandle` and returns a `BoxActiveLease`
//! to the trait caller. The sync FFI path leaks the Arc via
//! `Arc::into_raw` and hands the pointer back as `WatchHandleBox`;
//! `lease_drop` reclaims via `Arc::from_raw`. Both paths share
//! the same underlying state — sync renew/release dereference
//! without claiming ownership, the final `lease_drop` decrements
//! the last refcount.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::SecondsFormat;
use etcd_client::Client as EtcdClient;
use mcpg_cluster_api::{ActiveLease, ClusterError};
use mcpg_plugin_protocol::async_trait;
use mcpg_plugin_sdk::ffi::WatchHandleBox;
use tokio::runtime::Handle as RuntimeHandle;
use tokio::task::AbortHandle;
use tokio::time::sleep;

/// Lease state shared between async-trait callers and the FFI
/// pointer. Held behind `Arc` so the renewal task can outlive the
/// trait-object handle and the sync FFI's leaked pointer can
/// coexist with the async holder without double-free.
pub(crate) struct LeaseState {
    pub(crate) client: Arc<tokio::sync::Mutex<EtcdClient>>,
    pub(crate) lease_id: i64,
    pub(crate) lock_key: Vec<u8>,
    pub(crate) expires_at: StdMutex<String>,
    pub(crate) released: AtomicBool,
    /// Renewal task abort handle. Aborts on `LeaseState` drop —
    /// even if the host loses every handle clone, plugin shutdown
    /// drops the bundled tokio runtime which aborts everything.
    pub(crate) renewal_abort: StdMutex<Option<AbortHandle>>,
}

impl Drop for LeaseState {
    fn drop(&mut self) {
        if let Some(h) = self.renewal_abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// Async wrapper that exposes a `ActiveLease` trait object backed
/// by `Arc<LeaseState>`. Used by the in-tree async path; the
/// trait-object holder + the sync FFI leaked pointer both share
/// the same underlying state.
pub(crate) struct EtcdLeaseHandle(pub(crate) Arc<LeaseState>);

#[async_trait]
impl ActiveLease for EtcdLeaseHandle {
    fn fencing_token(&self) -> u64 {
        self.0.lease_id as u64
    }

    fn expires_at(&self) -> String {
        self.0.expires_at.lock().unwrap().clone()
    }

    async fn renew(&self) -> Result<(), ClusterError> {
        renew_state(&self.0).await.map(|_| ())
    }

    async fn release(&self) -> Result<(), ClusterError> {
        release_state(&self.0).await
    }
}

// ---------------------------------------------------------------------------
// Acquire — shared by async + sync paths
// ---------------------------------------------------------------------------

pub(crate) async fn acquire_async(
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    name: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Arc<LeaseState>, ClusterError> {
    if name.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease key must not be empty".into(),
        });
    }
    // Contend on the SAME `<name>/lock` create-revision txn that
    // `try_acquire_async` uses, by polling it with backoff until the key
    // is free. Polling the one key (rather than etcd's native `c.lock()`,
    // whose key shape is invisible to the try-variant's txn) keeps the
    // blocking and try variants mutually exclusive — otherwise both could
    // own the "same" lock simultaneously (split-brain). Blocking
    // semantics are preserved: this waits indefinitely.
    let mut backoff = Duration::from_millis(50);
    const MAX_BACKOFF: Duration = Duration::from_secs(2);
    loop {
        match try_acquire_async(
            Arc::clone(&client),
            name.clone(),
            ttl,
            renew_before_expiry_percent,
        )
        .await?
        {
            Some(state) => return Ok(state),
            None => {
                sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

pub(crate) fn acquire_sync(
    runtime: &RuntimeHandle,
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    name: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state = runtime.block_on(async move {
        acquire_async(client, name, ttl, renew_before_expiry_percent).await
    })?;
    wrap_state(state)
}

/// Non-blocking acquire via etcd transaction. Compares
/// `create_revision(key) == 0` (key doesn't exist); if true,
/// puts the key with a fresh lease. If false, returns
/// `Ok(None)` — another holder owns the key.
///
/// Uses the `<name>/lock` key directly (skipping etcd's blocking
/// `lock` API). Renewal task has the same shape as the blocking
/// path.
pub(crate) async fn try_acquire_async(
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    name: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Option<Arc<LeaseState>>, ClusterError> {
    if name.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease key must not be empty".into(),
        });
    }
    let ttl_secs = ttl.as_secs().max(1) as i64;
    let lock_key_str = format!("{name}/lock");

    // 1) Grant a lease + 2) txn put-if-not-exists.
    let (lease_id, lock_key) = {
        let mut c = client.lock().await;
        let lease =
            c.lease_grant(ttl_secs, None)
                .await
                .map_err(|e| ClusterError::BackendUnavailable {
                    reason: format!("lease_grant: {e}"),
                })?;
        let lease_id = lease.id();

        let txn = etcd_client::Txn::new()
            .when(vec![etcd_client::Compare::create_revision(
                lock_key_str.as_str(),
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then(vec![etcd_client::TxnOp::put(
                lock_key_str.as_str(),
                Vec::<u8>::new(),
                Some(etcd_client::PutOptions::new().with_lease(lease_id)),
            )])
            .or_else(vec![]);

        let resp = c
            .txn(txn)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("txn: {e}"),
            })?;

        if !resp.succeeded() {
            // Another holder. Drop the unused lease so etcd
            // doesn't carry it for `ttl_secs`.
            let _ = c.lease_revoke(lease_id).await;
            return Ok(None);
        }
        (lease_id, lock_key_str.into_bytes())
    };

    let expires_at = StdMutex::new(rfc3339_after(ttl));
    let state = Arc::new(LeaseState {
        client: Arc::clone(&client),
        lease_id,
        lock_key,
        expires_at,
        released: AtomicBool::new(false),
        renewal_abort: StdMutex::new(None),
    });

    // Same renewal task shape as `acquire_async`.
    let pct = renew_before_expiry_percent.clamp(1, 99);
    let sleep_for = ttl.saturating_mul(100u32.saturating_sub(pct)) / 100;
    let sleep_for = if sleep_for.is_zero() {
        Duration::from_millis(100)
    } else {
        sleep_for
    };
    let renewal_state = Arc::clone(&state);
    let join = RuntimeHandle::current().spawn(async move {
        loop {
            sleep(sleep_for).await;
            if renewal_state.released.load(Ordering::SeqCst) {
                break;
            }
            if renew_state(&renewal_state).await.is_err() {
                break;
            }
        }
    });
    let abort = join.abort_handle();
    *state.renewal_abort.lock().unwrap() = Some(abort);
    Ok(Some(state))
}

pub(crate) fn try_acquire_sync(
    runtime: &RuntimeHandle,
    client: Arc<tokio::sync::Mutex<EtcdClient>>,
    name: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state_opt = runtime.block_on(async move {
        try_acquire_async(client, name, ttl, renew_before_expiry_percent).await
    })?;
    match state_opt {
        Some(s) => wrap_state(s).map(Some),
        None => Ok(None),
    }
}

fn wrap_state(state: Arc<LeaseState>) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let token = state.lease_id as u64;
    let expires = state.expires_at.lock().unwrap().clone();
    let raw = Arc::into_raw(state);
    Ok((WatchHandleBox(raw as *mut ()), token, expires))
}

// ---------------------------------------------------------------------------
// Renew + release — used by both paths
// ---------------------------------------------------------------------------

pub(crate) async fn renew_state(state: &LeaseState) -> Result<String, ClusterError> {
    if state.released.load(Ordering::SeqCst) {
        return Err(ClusterError::LeaseExpired);
    }
    let mut c = state.client.lock().await;
    let resp =
        c.lease_keep_alive(state.lease_id)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("lease_keep_alive: {e}"),
            })?;
    // lease_keep_alive returns (LeaseKeeper, LeaseKeepAliveStream).
    // Send one heartbeat + read the response synchronously.
    let (mut keeper, mut stream) = resp;
    keeper
        .keep_alive()
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("keep_alive heartbeat: {e}"),
        })?;
    let kar = stream
        .message()
        .await
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("keep_alive recv: {e}"),
        })?;
    let granted_ttl = kar.map(|r| r.ttl()).unwrap_or(0);
    if granted_ttl <= 0 {
        // etcd returns ttl == 0 when the lease has been revoked.
        state.released.store(true, Ordering::SeqCst);
        return Err(ClusterError::LeaseExpired);
    }
    let new_expires = rfc3339_after(Duration::from_secs(granted_ttl as u64));
    *state.expires_at.lock().unwrap() = new_expires.clone();
    Ok(new_expires)
}

pub(crate) async fn release_state(state: &LeaseState) -> Result<(), ClusterError> {
    if state.released.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    if let Some(h) = state.renewal_abort.lock().unwrap().take() {
        h.abort();
    }
    let mut c = state.client.lock().await;
    // Best-effort unlock; lease_revoke is the authoritative
    // teardown.
    let _ = c.unlock(state.lock_key.clone()).await;
    let _ = c.lease_revoke(state.lease_id).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync FFI lease_renew / lease_release / lease_drop helpers
// ---------------------------------------------------------------------------

/// SAFETY: caller MUST pass a `WatchHandleBox` produced by
/// `acquire_sync`. The pointer is valid for the duration of the
/// borrow (host vtable contract — handle hasn't been dropped yet).
pub(crate) unsafe fn borrow_state(handle: &WatchHandleBox) -> Option<Arc<LeaseState>> {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return None;
    }
    // Increment the refcount so the caller's clone outlives the
    // borrow without claiming ownership of the leaked Arc.
    unsafe {
        Arc::increment_strong_count(ptr);
        Some(Arc::from_raw(ptr))
    }
}

/// SAFETY: caller MUST pass exactly one `WatchHandleBox` per
/// `acquire_sync` and never re-use it.
pub(crate) unsafe fn drop_state(handle: WatchHandleBox) {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return;
    }
    // Reclaim ownership of the Arc that was leaked in `wrap_state`.
    unsafe {
        let _ = Arc::from_raw(ptr);
    }
}

pub(crate) fn renew_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<String, ClusterError> {
    // SAFETY: host contract — `handle` produced by acquire_sync,
    // not yet dropped.
    let state = unsafe { borrow_state(&handle) }.ok_or(ClusterError::LeaseExpired)?;
    runtime.block_on(async move { renew_state(&state).await })
}

pub(crate) fn release_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<(), ClusterError> {
    let state = unsafe { borrow_state(&handle) };
    let state = match state {
        Some(s) => s,
        None => return Ok(()),
    };
    runtime.block_on(async move { release_state(&state).await })
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

fn rfc3339_after(ttl: Duration) -> String {
    let dt = chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}
