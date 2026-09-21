# Migrating 959K blobs from Kubo/IPFS to Iroh in production

*How we moved a media platform off Kubo + IPFS Cluster onto Iroh — 959K+
content items, ~500GB, 30+ dependent services — with zero downtime and a
rollback plan we never needed.*

## Why we left Kubo

Kubo is a fine piece of software, but we were running it as a *content
store*, not as a DHT participant. Our platform pins media (videos up to
302MB, images, thumbnails, documents) and serves it to ~30 internal
services through the Kubo HTTP API and `/ipfs/<CID>` gateway URLs.

The operational cost was the problem:

- **Memory**: bitswap + DHT + provider records consumed gigabytes of RAM
  for functionality we never used — nobody was fetching our content over
  the public IPFS network.
- **GC pressure**: Kubo's GC on ~1M pinned blocks is slow and scary.
  Iroh's reference counting is trivially simple by comparison.
- **Ops complexity**: IPFS Cluster for pin orchestration, DHT bootstraps,
  reproviding schedules — all machinery for a distribution problem we
  don't have.

Iroh's blob store is exactly what we needed: BLAKE3-verified content-
addressed storage with a minimal operational footprint.

## The hard part: CIDs

Everything else about the migration is plumbing. The hard part is that
**Iroh and IPFS disagree about what a content identifier is.**

- IPFS CIDv0 (`Qm…`) = base58(sha2-256 multihash of a DAG-PB block)
- Iroh = BLAKE3 hash of raw content (well, of a bao-tree over the content)

These are irreconcilable at the hash level: you cannot compute one from
the other, and you cannot "convert" a CIDv0 into a BLAKE3 hash. Every
bookmark, URL, database row, and service config that references `Qm…`
needs a translation layer — or a rewrite.

There's a subtler issue too: **Kubo doesn't store flat files.** Anything
over ~256KB is chunked into a UnixFS DAG-PB tree, so the root CID
addresses a DAG, not bytes. Migration means reassembling leaf blocks into
flat content *before* hashing — the root CID then maps to the BLAKE3 of
the reassembled bytes.

## What we built

### CID → BLAKE3 mapping (MongoDB)

```
cid_iroh_mappings: 959K documents
{ _id: "Qm…", cid: "Qm…", iroh_hash: "<blake3 hex>", size, pinned, migrated_at }
```

Loaded into memory at shim startup (~200MB for 959K entries), lookups are
O(1) and serve in <1ms.

### Kubo-compatible HTTP shim

~500 lines of Python/aiohttp implementing the subset of the Kubo API our
services actually call: `/api/v0/cat`, `add`, `pin/ls`, `pin/add`,
`block/stat`, `block/get`, `version`, `id`, `repo/stat`, `resolve`,
`object/stat`, `dag/stat`, `stats/bw`, `swarm/peers`, `bitswap/stat` —
plus `GET /ipfs/<CID>` with HTTP Range support (206 Partial Content),
which turned out to be essential: video seeking doesn't work without it.

The shim reads blobs directly from Iroh's `FsStore` data directory
(`blobs/data/<hash>.data`) — no iroh node in the request path.

One decision we'd now make differently: for content added *after*
migration we originally returned the raw 64-char BLAKE3 hex as the "CID".
A better choice — now implemented — is returning a **CIDv1** (`bafk…`,
raw codec + BLAKE3 multihash). It's a real CID that decodes back to the
hash deterministically, works with CID-parsing tooling, and needs no
mapping entry. Only CIDv0 genuinely requires the lookup table.

### The migrator

A Rust tool that walks Kubo's pin list and writes into Iroh's store.
Two content paths:

- **Kubo API mode** (default): `/api/v0/cat` — Kubo reassembles the DAG,
  we hash and store the flat bytes.
- **Direct mode** (`--direct`): reads `.ipldblock` files from Kubo's
  `blocks/` directory and reassembles DAG-PB ourselves — for when Kubo
  must stay offline.

Both write proper FsStore entries: `.data` file, `.obao4` bao-tree
outboard, and a batched `EntryState` commit into the `redb` database — so
the blobs are visible to a real iroh node, not just to the shim.

| Approach | Throughput | Bottleneck |
|----------|-----------|------------|
| `iroh blobs add` subprocess per CID | ~12 CID/s | process spawn + QUIC RPC |
| In-process migrator | ~100+ CID/s, ~50+ MB/s | disk I/O |

959K items: ~22 hours projected the naive way, ~2.7 hours actual.

## The cutover

1. Run the migrator against live Kubo (API mode) — hours, no downtime.
2. Re-run with `--skip-existing` to catch stragglers added during the
   first pass.
3. Repoint Traefik: `ipfs.*`, `cid.*`, `ipfs-api.*` → iroh-shim.
4. Keep Kubo's blockstore read-only on disk for rollback; the routes can
   flip back in seconds.

Rollback was tested, never needed.

## What we'd tell the next migrator

- **Audit your API surface first.** Our 30+ services used ~15 Kubo
  endpoints — a fraction of the full API. Build the shim for what you
  actually call, not the spec.
- **Range requests are non-negotiable** if you serve media. Verify 206s
  with `Content-Range` before cutover, not after.
- **Don't fight the hash change.** A mapping table is boring and it
  works. CIDv1-BLAKE3 for new content keeps you CID-compatible going
  forward.
- **Write FsStore entries properly** (outboard + redb), not just `.data`
  files — otherwise a real iroh node can't see your migrated content.
- **Keep the old blockstore.** It costs disk and buys you a rollback
  story that's one Traefik reload away.

## What's missing in Iroh for migrators

Shared upstream as [iroh-blobs#262](https://github.com/n0-computer/iroh-blobs/issues/262):
there's no Kubo API compatibility layer (fair — it doesn't belong in
iroh-blobs itself), no CIDv0 addressing (impossible by design), and no
migration tooling or guide. The [iroh-gateway example](https://github.com/n0-computer/iroh-examples/tree/main/iroh-gateway)
does cover HTTP serving with Range support — the remaining glue is the
`/api/v0/*` surface and the legacy-CID lookup, which is what this
repository provides.

---

*Code: [github.com/karawitan/kubo-to-iroh-migration](https://github.com/karawitan/kubo-to-iroh-migration) —
shim, migrator, architecture docs, and the validation test plan covering
all 79 Traefik-defined services.*
