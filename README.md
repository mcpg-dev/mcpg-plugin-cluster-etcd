# etcd Cluster Coordinator — `dev.mcpg.cluster.etcd`

> class `cluster` · `native` · package `mcpg-plugin-cluster-etcd` · artifact `libmcpg_plugin_cluster_etcd.so`

etcd-backed cluster coordinator. Talks to etcd v3 over
gRPC for durable+replayable pub/sub (via etcd Watch streams),
leadership election + distributed locks (native lease grant + lock),
and `watch_peers` (Watch on the `peers/` prefix). Reach for it when you
want stronger pub/sub delivery than Consul's gossip events offer.

## What it does
- **Peer discovery (read-only).** `list_peers` / `watch_peers` scan +
  watch `<prefix>peers/`, but **this plugin self-registers nothing** — those keys
  must be populated by an external process, else
  the peer set is empty. etcd is a coordination-only coordinator
  (leases/locks); use `redis` or `single_node` if you need gateway
  membership.
- **Leases** — `acquire_leadership` / `acquire_lock` + renew /
  release via etcd lease grant + lock, with keep-alive renewal.
- **Pub/sub** over etcd Watch streams — durable and replayable
  within the event retention window.
- Background renewal fires at `ttl × (100 − pct) / 100`.

## Configuration
Selected via the dedicated top-level `cluster:` block (`cluster.kind`).
Kind-specific fields are written **flat** under `cluster:` and flow
to the plugin's factory as JSON; the cdylib is still declared in the
`plugins:` list, where the inline `cluster.*` fields override any
`config:` on the matching entry.

```yaml
cluster:
  kind: etcd
  endpoints: ["https://etcd-0:2379", "https://etcd-1:2379"]  # required; explicit scheme
  key_prefix: /mcpg/                # MUST end in '/'; isolate deployments per prefix
  tls:                              # required for https:// endpoints
    ca_cert: /etc/mcpg/certs/etcd-ca.pem  # omit to use system roots
    # client_cert / client_key: ...       # for mTLS
    # domain_name: etcd.internal           # SNI / cert-name override
  # auth: { username: mcpg, password: ${env.ETCD_PW} }     # optional; needs https
  # node_id: gateway-pod-7          # optional; default <prefix>node-<hostname>
  # event_ttl_secs: 60                # transient pub/sub event TTL (seconds)
  # lease_renew_before_expiry_percent: 30

plugins:
  - id: dev.mcpg.cluster.etcd
    class: cluster
    source: { path: ./plugins/libmcpg_plugin_cluster_etcd.so }
```

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoints` | string[] | — (required) | etcd endpoints; the client load-balances + retries across them. Each MUST carry an explicit `http://` or `https://` scheme — a scheme-less `host:port` connects in clear and is rejected at boot. |
| `tls` | `{ca_cert?,client_cert?,client_key?,domain_name?}`? | none | TLS for `https://` endpoints. **Fail-closed:** an `https://` endpoint without `tls`, a `tls` block over `http://`-only endpoints, mixed `http`/`https`, or an mTLS half-pair (`client_cert` without `client_key`) are all rejected at boot. |
| `auth` | `{username,password}`? | none | Credentials for Auth-enabled clusters. **Rejected over plaintext `http://`** (the password would cross in clear) — use `https://` + `tls`. |
| `key_prefix` | string | `/mcpg/` | Key namespace for plugin data; MUST end in `/`. |
| `node_id` | string? | `<prefix>node-<hostname>` | Stable node identity. |
| `event_ttl_secs` | i64 | `60` | Transient pub/sub event TTL in **seconds** (>0). |
| `lease_renew_before_expiry_percent` | u32 | `30` | Renew at `100−pct`% of TTL; clamped to `[1,99]`. |

> **Transport security.** A multi-replica gateway refuses to boot
> against a plaintext etcd coordinator (any non-`https://` endpoint) unless
> `cluster.allow_insecure_transport: true` is set. Use `https://` + a `tls:`
> block for the secure default.

## Build
```bash
cargo build -p mcpg-plugin-cluster-etcd --features cdylib-export --release   # → target/release/libmcpg_plugin_cluster_etcd.so
```

Integration test (boots etcd, runs the shared equivalence suite):
`cargo test -p mcpg-plugin-cluster-etcd --features integration-tests`.

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Plugin system overview: `apps/gateway/docs/plugins.md`
- Full config reference: `apps/gateway/config.example.yaml`
