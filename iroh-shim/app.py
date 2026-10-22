"""
iroh-shim: Kubo-compatible HTTP API + gateway shim over Iroh blob store.

Translates:
  - GET  /ipfs/<CID>           → read blob from Iroh store
  - POST /api/v0/cat?arg=<CID>  → read blob from Iroh store
  - POST /api/v0/add            → store blob to Iroh store + write mapping
  - POST /api/v0/pin/add?arg=<CID>  → mark pinned in MongoDB
  - POST /api/v0/pin/ls         → list pinned CIDs from MongoDB
  - POST /api/v0/pin/rm?arg=<CID>   → unmark pinned in MongoDB
  - POST /api/v0/block/stat?arg=<CID> → return block size
  - POST /api/v0/ls?arg=<CID>   → return minimal listing
  - POST /api/v0/version        → return version info
  - POST /api/v0/id             → return node id
  - POST /api/v0/repo/stat      → return repo stats
  - POST /api/v0/stats/bw       → return empty bw stats
  - POST /api/v0/swarm/peers    → return empty peer list
  - POST /api/v0/bitswap/stat   → return empty bitswap stats

CID resolution order: CIDv1/bafk… (blake3 multihash) decodes natively;
CIDv0/legacy CIDs resolve via MongoDB (cid_iroh_mappings collection);
raw 64-hex hashes are accepted when the blob exists on disk.
Reads blobs from /data/iroh/blobs/data/<hash>.data
"""
import os
import sys
import time
import base64
import hashlib
import asyncio
from pathlib import Path

from aiohttp import web, ClientResponseError
import motor.motor_asyncio
import blake3
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# ── Config ──────────────────────────────────────────────────────────────────
IROH_DATA_DIR = Path(os.environ.get("IROH_DATA_DIR", "/data/iroh"))
BLOBS_DIR = IROH_DATA_DIR / "blobs" / "data"
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://mongo:27017/hub")
DB_NAME = os.environ.get("DB_NAME", "hub")
COLLECTION = os.environ.get("CID_MAPPINGS", "cid_iroh_mappings")

# ── Prometheus metrics ─────────────────────────────────────────────────────
REQUESTS_TOTAL = Counter(
    "iroh_shim_requests_total",
    "Total requests by endpoint and status",
    ["endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "iroh_shim_request_duration_seconds",
    "Request latency by endpoint",
    ["endpoint"],
    buckets=[0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
)
BYTES_SERVED = Counter(
    "iroh_shim_bytes_served_total",
    "Total bytes served by endpoint",
    ["endpoint"],
)
CACHE_HITS = Counter(
    "iroh_shim_cache_hits_total",
    "CID cache hits vs misses",
    ["result"],  # "hit" or "miss"
)
CACHE_SIZE = Gauge(
    "iroh_shim_cache_size",
    "Number of CID→iroh_hash mappings in memory cache",
)
BLOB_COUNT = Gauge(
    "iroh_shim_blob_count",
    "Number of .data blob files on disk",
)
BLOB_DISK_BYTES = Gauge(
    "iroh_shim_blob_disk_bytes",
    "Total size of .data blob files on disk (bytes)",
)
MAPPINGS_TOTAL = Gauge(
    "iroh_shim_mappings_total",
    "Total CID→iroh_hash mappings in MongoDB",
)
MAPPINGS_WITH_IROH_HASH = Gauge(
    "iroh_shim_mappings_with_iroh_hash",
    "Mappings that have an iroh_hash populated",
)
PINNED_COUNT = Gauge(
    "iroh_shim_pinned_count",
    "Number of pinned CIDs in MongoDB",
)

# ── MongoDB ────────────────────────────────────────────────────────────────
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = mongo_client[DB_NAME]
mappings = db[COLLECTION]

# ── CIDv1 (bafk…) ↔ BLAKE3 helpers ─────────────────────────────────────────
# An iroh blob hash is a BLAKE3 digest. It is also a valid CIDv1 multihash
# (multicodec 0x1e, 32-byte digest), so post-migration content can be
# addressed with real CIDs (bafkr4i…) instead of bare hex. Only CIDv0
# (Qm…, sha2-256 over DAG-PB) genuinely needs the MongoDB mapping.
BLAKE3_MULTICODEC = 0x1E
RAW_CODEC = 0x55


def _varint(data: bytes, pos: int) -> tuple[int, int] | None:
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
        if shift >= 64:
            return None
    return None


def hash_hex_to_cidv1(hash_hex: str) -> str:
    """Encode a BLAKE3 hash hex as CIDv1 (raw codec + blake3 multihash): bafk…"""
    cid_bytes = bytes([0x01, RAW_CODEC, BLAKE3_MULTICODEC, 0x20]) + bytes.fromhex(hash_hex)
    return "b" + base64.b32encode(cid_bytes).decode("ascii").lower().rstrip("=")


def cidv1_to_hash(cid: str) -> str | None:
    """Decode a CIDv1 carrying a BLAKE3 multihash (bafk…) to the hash hex.

    Returns None for CIDv0 or CIDv1 with a non-blake3 multihash — those need
    the MongoDB mapping. The codec field is ignored: any CIDv1 whose
    multihash is blake3-256 maps to the same blob.
    """
    if not cid.startswith("b"):
        return None
    body = cid[1:]
    try:
        raw = base64.b32decode(body.upper() + "=" * (-len(body) % 8))
    except Exception:
        return None
    pos = 0
    v = _varint(raw, pos)
    if v is None or v[0] != 0x01:
        return None
    pos = v[1]
    v = _varint(raw, pos)  # codec
    if v is None:
        return None
    pos = v[1]
    v = _varint(raw, pos)  # multihash code
    if v is None or v[0] != BLAKE3_MULTICODEC:
        return None
    pos = v[1]
    v = _varint(raw, pos)  # digest length
    if v is None or v[0] != 0x20:
        return None
    pos = v[1]
    if len(raw) - pos != 0x20:
        return None
    return raw[pos:].hex()


# ── In-memory cache: CID → iroh_hash (hex) ──────────────────────────────────
_cid_cache: dict[str, str] = {}
_cache_loaded = False


async def load_cache():
    """Load all CID→iroh_hash mappings into memory at startup."""
    global _cache_loaded
    if _cache_loaded:
        return
    count = 0
    async for doc in mappings.find({"iroh_hash": {"$exists": True}}, {"_id": 1, "iroh_hash": 1, "cid": 1}):
        cid = doc.get("cid") or doc.get("_id")
        if isinstance(cid, str) and doc.get("iroh_hash"):
            _cid_cache[cid] = doc["iroh_hash"]
            count += 1
    _cache_loaded = True
    print(f"[iroh-shim] Loaded {count} CID→iroh_hash mappings into cache", flush=True)


async def lookup_iroh_hash(cid: str) -> str | None:
    """Look up the Iroh hash for a CID, with cache.

    Handles four cases:
    1. CIDv1 with BLAKE3 multihash (bafk…) → decoded directly, no Mongo needed
    2. CIDv0/legacy CIDv1 (e.g., Qm…) → look up in cid_iroh_mappings.cid or _id
    3. Iroh hash (64 hex chars) passed directly → blob on disk, or
       cid_iroh_mappings.iroh_hash
    4. Cached result from previous lookup
    """
    if cid in _cid_cache:
        CACHE_HITS.labels(result="hit").inc()
        return _cid_cache[cid]
    # CIDv1 bafk… decodes to the hash deterministically — no mapping needed
    decoded = cidv1_to_hash(cid)
    if decoded:
        _cid_cache[cid] = decoded
        CACHE_HITS.labels(result="hit").inc()
        return decoded
    # Fallback to DB
    doc = await mappings.find_one({"$or": [{"cid": cid}, {"_id": cid}]})
    if doc and doc.get("iroh_hash"):
        _cid_cache[cid] = doc["iroh_hash"]
        CACHE_HITS.labels(result="miss").inc()
        return doc["iroh_hash"]
    # If the input looks like an iroh hash (64 hex chars), accept it directly
    # when the blob exists — post-migration content may have no mapping entry
    if len(cid) == 64 and all(c in "0123456789abcdef" for c in cid.lower()):
        if (BLOBS_DIR / f"{cid.lower()}.data").exists():
            _cid_cache[cid] = cid.lower()
            CACHE_HITS.labels(result="miss").inc()
            return cid.lower()
        doc = await mappings.find_one({"iroh_hash": cid})
        if doc and doc.get("iroh_hash"):
            _cid_cache[cid] = doc["iroh_hash"]
            CACHE_HITS.labels(result="miss").inc()
            return doc["iroh_hash"]
    CACHE_HITS.labels(result="miss").inc()
    return None


async def store_mapping(cid: str, iroh_hash: str, size: int):
    """Store a CID→iroh_hash mapping in MongoDB and cache."""
    _cid_cache[cid] = iroh_hash
    await mappings.update_one(
        {"$or": [{"cid": cid}, {"_id": cid}]},
        {"$set": {
            "cid": cid,
            "iroh_hash": iroh_hash,
            "size": size,
            "migrated_at": time.time(),
            "pinned": True,
        }},
        upsert=True,
    )


# ── Iroh blob read/write ────────────────────────────────────────────────────

def compute_iroh_hash(data: bytes) -> str:
    """Compute Iroh BLAKE3 hash (the root hash, same as standard blake3)."""
    h = blake3.blake3()
    h.update(data)
    return h.hexdigest()


def read_blob(iroh_hash: str) -> bytes | None:
    """Read a blob from the Iroh store."""
    path = BLOBS_DIR / f"{iroh_hash}.data"
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def write_blob(data: bytes) -> str:
    """Write a blob to the Iroh store, return hash_hex."""
    hash_hex = compute_iroh_hash(data)
    data_path = BLOBS_DIR / f"{hash_hex}.data"
    data_path.write_bytes(data)
    return hash_hex


# ── HTTP handlers ──────────────────────────────────────────────────────────

async def gateway_ipfs(request: web.Request) -> web.StreamResponse:
    """GET /ipfs/<CID> — serve content from Iroh store, with HTTP Range support."""
    cid = request.match_info["cid"]
    # Strip any path suffix (e.g. /ipfs/<CID>/filename)
    cid = cid.split("/")[0]
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        REQUESTS_TOTAL.labels(endpoint="gateway", status="404").inc()
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    blob_path = BLOBS_DIR / f"{iroh_hash}.data"
    if not blob_path.exists():
        REQUESTS_TOTAL.labels(endpoint="gateway", status="404").inc()
        raise web.HTTPNotFound(text=f"Blob not found: {iroh_hash}")
    total_size = blob_path.stat().st_size
    # Parse Range header for video seeking support (206 Partial Content)
    range_header = request.headers.get("Range")
    if range_header and range_header.startswith("bytes="):
        range_spec = range_header[6:].split(",")[0].strip()
        try:
            start_str, end_str = range_spec.split("-", 1)
            if start_str:
                start = int(start_str)
                end = int(end_str) if end_str else total_size - 1
            else:
                # Suffix range: bytes=-N means the last N bytes
                end = total_size - 1
                start = max(0, total_size - int(end_str))
            if end >= total_size:
                end = total_size - 1
            if start > end or start >= total_size:
                REQUESTS_TOTAL.labels(endpoint="gateway", status="416").inc()
                return web.Response(
                    status=416,
                    headers={"Content-Range": f"bytes */{total_size}"},
                )
            length = end - start + 1
            BYTES_SERVED.labels(endpoint="gateway").inc(length)
            REQUESTS_TOTAL.labels(endpoint="gateway", status="206").inc()
            # Use StreamResponse for Range requests — FileResponse overrides
            # status and Content-Length, breaking partial content
            resp = web.StreamResponse(
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(length),
                    "Content-Type": "application/octet-stream",
                },
            )
            await resp.prepare(request)
            with open(blob_path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(remaining, 1024 * 1024))
                    if not chunk:
                        break
                    await resp.write(chunk)
                    remaining -= len(chunk)
            await resp.write_eof()
            return resp
        except (ValueError, IndexError):
            pass  # Fall through to full response
    BYTES_SERVED.labels(endpoint="gateway").inc(total_size)
    REQUESTS_TOTAL.labels(endpoint="gateway", status="200").inc()
    return web.FileResponse(
        str(blob_path),
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(total_size),
        },
    )


async def api_cat(request: web.Request) -> web.Response:
    """POST /api/v0/cat?arg=<CID> — return content bytes."""
    cid = request.query.get("arg", "")
    if not cid:
        REQUESTS_TOTAL.labels(endpoint="cat", status="400").inc()
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        REQUESTS_TOTAL.labels(endpoint="cat", status="404").inc()
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    data = read_blob(iroh_hash)
    if data is None:
        REQUESTS_TOTAL.labels(endpoint="cat", status="404").inc()
        raise web.HTTPNotFound(text=f"Blob not found: {iroh_hash}")
    BYTES_SERVED.labels(endpoint="cat").inc(len(data))
    REQUESTS_TOTAL.labels(endpoint="cat", status="200").inc()
    return web.Response(body=data)


async def api_add(request: web.Request) -> web.Response:
    """POST /api/v0/add — accept multipart upload, store to Iroh, return CID."""
    reader = await request.multipart()
    field = await reader.next()
    if field is None:
        raise web.HTTPBadRequest(text="No file in upload")
    data = await field.read()
    hash_hex = write_blob(data)
    # We can't compute a CIDv0 (Qm…) — that requires sha2-256 over DAG-PB.
    # Emit a CIDv1 (bafk…) instead: a real CID that decodes back to the
    # BLAKE3 hash, so it works with CID-parsing tooling and resolves here
    # without a MongoDB lookup.
    cid = hash_hex_to_cidv1(hash_hex)
    await store_mapping(cid, hash_hex, len(data))
    return web.json_response({
        "Name": field.filename or "file",
        "Hash": cid,
        "Size": str(len(data)),
    })


async def api_add_only_hash(request: web.Request) -> web.Response:
    """POST /api/v0/add?only-hash=true — compute hash without storing."""
    reader = await request.multipart()
    field = await reader.next()
    if field is None:
        raise web.HTTPBadRequest(text="No file in upload")
    data = await field.read()
    hash_hex = compute_iroh_hash(data)
    cid = hash_hex_to_cidv1(hash_hex)
    return web.json_response({
        "Name": field.filename or "file",
        "Hash": cid,
        "Size": str(len(data)),
    })


async def api_pin_add(request: web.Request) -> web.Response:
    """POST /api/v0/pin/add?arg=<CID> — mark as pinned."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    doc = await mappings.find_one({"$or": [{"cid": cid}, {"_id": cid}]}, {"_id": 1})
    if doc:
        await mappings.update_one({"_id": doc["_id"]}, {"$set": {"pinned": True}})
    else:
        await mappings.insert_one({"_id": cid, "cid": cid, "pinned": True})
    return web.json_response({"Pins": [cid]})


async def api_pin_ls(request: web.Request) -> web.Response:
    """POST /api/v0/pin/ls — list pinned CIDs."""
    pinned = {}
    async for doc in mappings.find({"pinned": True}, {"cid": 1}):
        cid = doc.get("cid") or doc.get("_id")
        if isinstance(cid, str):
            pinned[cid] = {"Type": "recursive"}
    return web.json_response({"Keys": pinned})


async def api_pin_rm(request: web.Request) -> web.Response:
    """POST /api/v0/pin/rm?arg=<CID> — unmark pinned."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    await mappings.update_one(
        {"$or": [{"cid": cid}, {"_id": cid}]}, {"$set": {"pinned": False}}
    )
    return web.json_response({"Pins": [cid]})


async def api_block_stat(request: web.Request) -> web.Response:
    """POST /api/v0/block/stat?arg=<CID> — return block size."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    data = read_blob(iroh_hash)
    if data is None:
        raise web.HTTPNotFound(text=f"Blob not found: {iroh_hash}")
    return web.json_response({"Key": cid, "Size": str(len(data))})


async def api_ls(request: web.Request) -> web.Response:
    """POST /api/v0/ls?arg=<CID> — return minimal listing."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    data = read_blob(iroh_hash)
    size = len(data) if data else 0
    return web.json_response({
        "Objects": [{
            "Hash": cid,
            "Size": size,
            "Type": "file",
            "Links": [],
        }]
    })


async def api_version(request: web.Request) -> web.Response:
    return web.json_response({
        "Version": "0.0.1-iroh-shim",
        "Commit": "iroh",
        "System": "amd64",
        "Golang": "iroh",
    })


async def api_id(request: web.Request) -> web.Response:
    return web.json_response({
        "ID": "iroh-shim",
        "Addresses": [],
        "Protocols": [],
    })


async def api_repo_stat(request: web.Request) -> web.Response:
    # Count blobs on disk
    num_objects = 0
    repo_size = 0
    try:
        for p in BLOBS_DIR.glob("*.data"):
            num_objects += 1
            try:
                repo_size += p.stat().st_size
            except OSError:
                pass
    except Exception:
        pass
    return web.json_response({
        "RepoSize": repo_size,
        "StorageMax": 0,
        "NumObjects": num_objects,
    })


async def api_stats_bw(request: web.Request) -> web.Response:
    return web.json_response({
        "TotalIn": 0,
        "TotalOut": 0,
        "RateIn": 0,
        "RateOut": 0,
    })


async def api_swarm_peers(request: web.Request) -> web.Response:
    return web.json_response({"Peers": []})


async def api_bitswap_stat(request: web.Request) -> web.Response:
    return web.json_response({
        "Wantlist": [],
        "BlocksReceived": 0,
        "BlocksSent": 0,
        "Peers": [],
    })


async def api_block_get(request: web.Request) -> web.Response:
    """POST /api/v0/block/get?arg=<CID> — return raw block."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    data = read_blob(iroh_hash)
    if data is None:
        raise web.HTTPNotFound(text=f"Blob not found: {iroh_hash}")
    return web.Response(body=data)


async def api_resolve(request: web.Request) -> web.Response:
    """POST /api/v0/resolve?arg=<CID> — return /ipfs/<CID>."""
    cid = request.query.get("arg", "")
    return web.json_response({"Path": f"/ipfs/{cid}"})


async def api_object_stat(request: web.Request) -> web.Response:
    """POST /api/v0/object/stat?arg=<CID> — return minimal stat."""
    cid = request.query.get("arg", "")
    iroh_hash = await lookup_iroh_hash(cid)
    if not iroh_hash:
        raise web.HTTPNotFound(text=f"CID not found: {cid}")
    data = read_blob(iroh_hash)
    size = len(data) if data else 0
    return web.json_response({
        "Hash": cid,
        "NumLinks": 0,
        "BlockSize": size,
        "LinksSize": 0,
        "DataSize": size,
        "CumulativeSize": size,
    })


async def api_dag_stat(request: web.Request) -> web.Response:
    return await api_object_stat(request)


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "iroh-shim"})


async def metrics(request: web.Request) -> web.Response:
    """GET /metrics — Prometheus metrics endpoint."""
    return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST)


async def update_gauges():
    """Periodically update gauge metrics (blob count, disk usage, mapping counts)."""
    while True:
        try:
            # Blob count and disk usage
            blob_count = 0
            blob_bytes = 0
            if BLOBS_DIR.exists():
                for f in BLOBS_DIR.glob("*.data"):
                    blob_count += 1
                    try:
                        blob_bytes += f.stat().st_size
                    except OSError:
                        pass
            BLOB_COUNT.set(blob_count)
            BLOB_DISK_BYTES.set(blob_bytes)

            # MongoDB mapping counts
            total = await mappings.count_documents({})
            with_hash = await mappings.count_documents({"iroh_hash": {"$exists": True}})
            pinned = await mappings.count_documents({"pinned": True})
            MAPPINGS_TOTAL.set(total)
            MAPPINGS_WITH_IROH_HASH.set(with_hash)
            PINNED_COUNT.set(pinned)

            # Cache size
            CACHE_SIZE.set(len(_cid_cache))
        except Exception as e:
            print(f"[iroh-shim] Gauge update error: {e}", flush=True)
        await asyncio.sleep(30)


# ── App setup ──────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application(client_max_size=1024 * 1024 * 1024)  # 1GB
    # Gateway
    app.router.add_get("/ipfs/{cid}", gateway_ipfs)
    # Kubo API (POST endpoints, query param style)
    app.router.add_post("/api/v0/cat", api_cat)
    app.router.add_post("/api/v0/add", api_add)
    app.router.add_post("/api/v0/pin/add", api_pin_add)
    app.router.add_post("/api/v0/pin/ls", api_pin_ls)
    app.router.add_post("/api/v0/pin/rm", api_pin_rm)
    app.router.add_post("/api/v0/block/stat", api_block_stat)
    app.router.add_post("/api/v0/block/get", api_block_get)
    app.router.add_post("/api/v0/ls", api_ls)
    app.router.add_post("/api/v0/version", api_version)
    app.router.add_post("/api/v0/id", api_id)
    app.router.add_post("/api/v0/repo/stat", api_repo_stat)
    app.router.add_post("/api/v0/stats/bw", api_stats_bw)
    app.router.add_post("/api/v0/swarm/peers", api_swarm_peers)
    app.router.add_post("/api/v0/bitswap/stat", api_bitswap_stat)
    app.router.add_post("/api/v0/resolve", api_resolve)
    app.router.add_post("/api/v0/object/stat", api_object_stat)
    app.router.add_post("/api/v0/dag/stat", api_dag_stat)
    # Health
    app.router.add_get("/health", health)
    # Prometheus metrics
    app.router.add_get("/metrics", metrics)
    return app


async def startup(app: web.Application):
    await load_cache()
    # Start background gauge updater
    asyncio.create_task(update_gauges())


if __name__ == "__main__":
    app = create_app()
    app.on_startup.append(startup)
    port = int(os.environ.get("PORT", "5001"))
    web.run_app(app, host="0.0.0.0", port=port, access_log=None)
