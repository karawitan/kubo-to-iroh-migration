# Architecture

## Overview

The migration replaces Kubo/IPFS + IPFS Cluster with Iroh as the content store,
while preserving compatibility with existing CID references, URLs, and
dependent services through a shim layer.

## Before (Kubo/IPFS)

```
┌──────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Dependent   │────▶│    Traefik    │────▶│   Kubo (IPFS)   │
│  services    │     │  (routing)    │     │  /api/v0/*      │
│  (30+)       │     └──────────────┘     │  /ipfs/<CID>    │
└──────────────┘                          │  bitswap/DHT   │
                                           └────────┬───────┘
                                                    │
                                           ┌────────▼───────┐
                                           │  IPFS Cluster  │
                                           │  (pinning)      │
                                           └────────────────┘
```

Problems: high memory (DHT + bitswap), complex ops, GC pressure, slow provider
discovery for nodes without DHT routing.

## After (Iroh)

```
┌──────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Dependent   │────▶│    Traefik    │────▶│   iroh-shim    │
│  services    │     │  (routing)    │     │  /api/v0/*      │
│  (30+)       │     └──────────────┘     │  /ipfs/<CID>    │
│  (unchanged) │                          │  /metrics       │
└──────────────┘                          └────────┬───────┘
                                                   │
                                          ┌────────▼───────┐
                                          │  MongoDB       │
                                          │  cid_iroh_      │
                                          │  mappings       │
                                          └────────┬───────┘
                                                   │
                                          ┌────────▼───────┐
                                          │  Iroh FsStore  │
                                          │  (redb backend)│
                                          │  /data/iroh/   │
                                          │  blobs/data/   │
                                          └────────────────┘
```

Benefits: lower memory, simpler ops, no DHT, no GC pressure, BLAKE3 verified
streaming.

## Components

### iroh-shim

The shim is the compatibility layer that makes the migration transparent to
dependent services. It implements:

1. **Kubo HTTP API** — existing services call `/api/v0/cat`, `/api/v0/add`,
   `/api/v0/pin/ls`, etc. The shim translates these to Iroh blob operations.

2. **IPFS gateway** — `GET /ipfs/<CID>` serves content from the Iroh blob
   store, with HTTP Range support (206 Partial Content) for video seeking.

3. **CID resolution** — legacy CIDs (CIDv0 `Qm...`) are looked up in MongoDB's
   `cid_iroh_mappings` collection to find the corresponding BLAKE3 hash. The
   blob is then read from `/data/iroh/blobs/data/<hash>.data`.

4. **Metrics** — Prometheus `/metrics` endpoint for monitoring.

### MongoDB mapping

```
Collection: cid_iroh_mappings
Schema:
  _id:        <CIDv0 string>           # e.g., "QmNLexjuJXxxnsAarYUGejf6gacAkmcvwbs4RWtgpxq932"
  cid:        <CIDv0 string>          # duplicate of _id for query convenience
  iroh_hash:  <BLAKE3 hex string>     # e.g., "746a4c34de0949197829cb83f16dca6922c4f874e401b0d4daec4d5b1a454796"
  size:       <int64>                 # content size in bytes
  pinned:     <bool>                  # whether the CID is pinned
  migrated_at: <datetime>             # migration timestamp
```

The shim caches mappings in memory (959K entries, ~200MB RAM) for O(1) lookup.
Cache hits serve in <1ms; misses fall back to MongoDB.

### Iroh blob store

Iroh's `FsStore` with `redb` backend stores blobs as flat files:

```
/data/iroh/
  blobs/
    data/
      <blake3-hex>.data    # raw blob content
    blobs.db                # redb metadata database
```

The shim reads blob files directly from the filesystem, bypassing Iroh's API
for maximum performance. This is safe because:

- Blobs are content-addressed (BLAKE3) — no collision risk
- The shim only reads, never writes to the data directory (writes go through
  `/api/v0/add` which writes to the same directory)
- The `redb` database is not needed for reads — it's Iroh's metadata index

### Offline migrator

The Rust utility reads Kubo's block store and writes to Iroh's blob store:

1. Read `.ipldblock` files from `~/.ipfs/blocks/`
2. Parse DAG-PB to extract UnixFS content
3. For files: unwrap to raw bytes
4. For directories: recursively process children
5. Compute BLAKE3 hash of raw content
6. Write `<blake3-hex>.data` to Iroh's `blobs/data/`
7. Record CID → BLAKE3 mapping in MongoDB

This is an offline operation — both Kubo and Iroh are stopped during
migration. The migrator processes blocks in parallel for throughput.

## HTTP Range support

Video streaming requires `206 Partial Content` responses. The shim parses
the `Range: bytes=start-end` header and returns:

```
HTTP/1.1 206 Partial Content
Content-Range: bytes 0-1023/302517127
Accept-Ranges: bytes
Content-Length: 1024
```

The response is streamed in 1MB chunks via `StreamResponse` (not
`FileResponse`, which overrides the status code and Content-Length).

Benchmarks (302MB video blob):
- 1KB range: 11ms TTFB
- 1MB range at 100MB offset: 700ms
- Full file: 4s (302MB)

## Traefik routing

The shim is exposed via Traefik with the same hostnames as the old Kubo
gateway:

- `ipfs.kalou.net` → iroh-shim:5001 (gateway)
- `ipns.kalou.net` → iroh-shim:5001 (gateway)
- `cid.kalou.net` → iroh-shim:5001 (gateway)
- `ipfs-api.kalou.net` → iroh-shim:5001 (Kubo API)

Subdomain `/ipfs/` routes (pornmap, porntoc, tvid, etc.) also point to
iroh-shim directly, bypassing the old nginx slice cache (which proxied to
Kubo).

## Rollback

The migration is designed for rollback:

1. CID mappings are preserved in MongoDB — no data is lost
2. Kubo's block store is preserved on disk (read-only during migration)
3. The old Traefik routes can be repointed back to Kubo in seconds
4. The shim can be scaled to 0 replicas to stop serving

Rollback was tested during development but not needed in production.
