# Kubo → Iroh Migration Kit

Reference implementation and tooling for migrating a production platform from
Kubo/IPFS + IPFS Cluster to [Iroh](https://iroh.computer), preserving existing
CID references, URLs, and dependent services.

## What's here

This repository contains the compatibility layer and migration tooling built
during a production cutover of a media platform with **959K+ content items**
and **30+ dependent services**.

### Components

| Component | Path | Description |
|-----------|------|-------------|
| **iroh-shim** | [`iroh-shim/`](iroh-shim/) | Kubo-compatible HTTP API + gateway shim over Iroh blob store |
| **offline-migrator** | [`offline-migrator/`](offline-migrator/) | Rust utility to migrate Kubo block store → Iroh blob store |
| **docs** | [`docs/`](docs/) | Architecture, migration guide, validation test plan |

### iroh-shim

A Python/aiohttp service that provides:

- **Kubo HTTP API compatibility**: `/api/v0/cat`, `/api/v0/add`, `/api/v0/pin/ls`,
  `/api/v0/pin/add`, `/api/v0/block/stat`, `/api/v0/block/get`, `/api/v0/version`,
  `/api/v0/id`, `/api/v0/repo/stat`, `/api/v0/resolve`, `/api/v0/object/stat`,
  `/api/v0/dag/stat`, `/api/v0/stats/bw`, `/api/v0/swarm/peers`, `/api/v0/bitswap/stat`
- **IPFS gateway**: `GET /ipfs/<CID>` with HTTP Range support (206 Partial Content)
- **CID → Iroh hash resolution**: Looks up legacy CIDs in a MongoDB mapping
  collection, resolves to BLAKE3 hashes, serves blobs from Iroh's `FsStore`
- **Prometheus metrics**: `/metrics` endpoint with request counters, latency
  histograms, cache hit/miss, blob count, disk usage, mapping count

The shim allows existing IPFS-dependent services to continue working without
modification after the underlying storage is migrated from Kubo to Iroh.

### offline-migrator

A Rust utility that reads Kubo's UnixFS/DAG-PB block store and writes flat
blobs to Iroh's blob store:

- Reads `.ipldblock` files from Kubo's `blocks/` directory
- Unwraps UnixFS DAG-PB to extract raw content
- Computes BLAKE3 hash of raw content
- Writes `<blake3-hex>.data` files to Iroh's `blobs/data/` directory
- Records CID → BLAKE3 mapping in MongoDB

This avoids re-uploading content through the API and preserves all existing
CID references.

## Migration architecture

```
                    ┌─────────────────────────────────────────┐
                    │           Dependent services            │
                    │  (indexers, pinners, web apps, etc.)    │
                    └──────────────┬──────────────────────────┘
                                   │ HTTP (Kubo API + gateway)
                    ┌──────────────▼──────────────────────────┐
                    │            iroh-shim                     │
                    │  /api/v0/* + /ipfs/<CID> + /metrics     │
                    │  CID→BLAKE3 lookup via MongoDB           │
                    │  HTTP Range (206) for video seeking      │
                    └──────────────┬──────────────────────────┘
                                   │ read blobs
                    ┌──────────────▼──────────────────────────┐
                    │     Iroh FsStore (redb backend)          │
                    │     /data/iroh/blobs/data/<hash>.data    │
                    └─────────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────┐
                    │  MongoDB: cid_iroh_mappings             │
                    │  { cid: "Qm...", iroh_hash: "abc...",   │
                    │    size, pinned, migrated_at }          │
                    └─────────────────────────────────────────┘
```

## Why

Migrating from Kubo/IPFS to Iroh offers:

- **Lower memory**: Iroh's blob store uses significantly less RAM than Kubo's
  bitswap + DHT
- **Simpler ops**: No DHT, no pubsub, no bootstrap peers — just a local blob store
- **BLAKE3 verified streaming**: Faster content addressing than SHA2-256/DAG-PB
- **No GC pressure**: Iroh's reference counting is simpler than Kubo's GC

The challenge is that existing services, URLs, bookmarks, and databases all
reference CIDs. This kit provides the compatibility layer to migrate without
rewriting every consumer.

## Production scale

| Metric | Value |
|--------|-------|
| Content items | 959K+ |
| Total storage | ~500 GB |
| Max blob size | 302 MB (video) |
| Dependent services | 30+ |
| CID mappings | 959K entries |
| Zero-downtime cutover | Yes (with rollback) |
| Video streaming | HLS with HTTP Range seeking |

## Related

- [Iroh](https://github.com/n0-computer/iroh) — the networking library
- [iroh-blobs](https://github.com/n0-computer/iroh-blobs) — blob storage crate
- [Upstream issue #262](https://github.com/n0-computer/iroh-blobs/issues/262) —
  discussion of Kubo compatibility patterns with the Iroh team

## License

MIT
