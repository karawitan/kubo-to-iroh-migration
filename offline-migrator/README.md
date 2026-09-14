# iroh-migrator

Fast migration tool for moving content from Kubo/IPFS to Iroh blob storage.

## Why

Migrating from Kubo/IPFS to Iroh requires re-importing all pinned content while preserving CID-to-Iroh hash mappings. The naive approach (spawning `iroh blobs add` per CID) is bottlenecked by process spawn + QUIC RPC overhead (~100ms per CID).

This tool provides:
- **In-process Iroh store access** (no subprocess spawn, no QUIC RPC)
- **Streaming from Kubo** (no temp files, no double copy)
- **Parallel workers** with configurable concurrency
- **MongoDB mapping persistence** for resume/checkpoint
- **WAL (Write-Ahead Log)** for crash recovery
- **`--force-all` mode** for re-importing when the Iroh store is wiped but mappings are kept

## Architecture

```
Kubo API ──→ fetch CID content ──→ Iroh blob store (in-process)
                                         │
                                   MongoDB mapping
                                   (CID → BLAKE3 hash)
```

## Usage

```bash
# Build
cargo build --release

# Run migration (8 workers, re-import all)
./target/release/iroh-migrator \
  --kubo-api http://127.0.0.1:5004/api/v0 \
  --mongo-uri mongodb://127.0.0.1:27017/hub \
  --iroh-dir /data/iroh \
  --workers 8 \
  --force-all

# Resume mode (skip already-mapped CIDs)
./target/release/iroh-migrator \
  --kubo-api http://127.0.0.1:5004/api/v0 \
  --mongo-uri mongodb://127.0.0.1:27017/hub \
  --iroh-dir /data/iroh \
  --workers 8
```

## Performance

| Approach | Throughput | Bottleneck |
|----------|-----------|------------|
| Python + `iroh` CLI subprocess | ~12 CID/s, ~5 MB/s | Process spawn + QUIC RPC |
| Rust in-process (this tool) | ~100+ CID/s, ~50+ MB/s | Disk I/O only |

## Requirements

- Rust 1.98+
- Kubo/IPFS daemon running with content pinned
- MongoDB for mapping storage
- Iroh blob store directory

## License

MIT
