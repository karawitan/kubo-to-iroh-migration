# iroh-kubo-shim

Rust port of [`iroh-shim`](../iroh-shim/) — a Kubo-compatible HTTP API and
IPFS gateway that serves blobs directly from an Iroh `FsStore` directory,
with CIDv0→BLAKE3 resolution via MongoDB.

## Differences from the Python shim

- **Proper FsStore writes**: `/api/v0/add` writes the `.data` file, the
  `.obao4` bao-tree outboard, and an `EntryState` record in the `redb`
  `blobs-0` table — so a real iroh node can serve content added through
  the shim. The Python shim writes only `.data`.
- **bao-tree root hashing**: content is hashed the way iroh hashes it
  (bao-tree root), not plain BLAKE3. The two coincide for blobs ≤1KiB and
  diverge above that — the Python shim's `/add` output is
  self-consistent but not iroh-native for larger blobs.
- **CIDv1 identifiers**: `/api/v0/add` returns `bafk…` CIDs (raw codec +
  blake3 multihash) instead of bare hex — parseable by any CID tooling
  and resolvable without a MongoDB lookup.

## CID resolution order

1. CIDv1 `bafk…` (blake3 multihash) → decoded deterministically, no DB hit
2. CIDv0 / legacy CIDv1 → MongoDB `cid_iroh_mappings` lookup
3. Raw 64-hex hash → accepted when the blob exists on disk

## Configuration

| Env var | Default |
|---------|---------|
| `IROH_DATA_DIR` | `/data/iroh` |
| `MONGO_URL` | `mongodb://mongo:27017/hub` |
| `DB_NAME` | `hub` |
| `CID_MAPPINGS` | `cid_iroh_mappings` |
| `PORT` | `5001` |

## Run

```bash
cargo run --release
```

Same endpoints as the Python shim: `GET /ipfs/{cid}` (with Range/206),
`POST /api/v0/{cat,add,pin/*,block/*,ls,version,id,repo/stat,stats/bw,
swarm/peers,bitswap/stat,resolve,object/stat,dag/stat}`, `GET /health`,
`GET /metrics` (Prometheus).
