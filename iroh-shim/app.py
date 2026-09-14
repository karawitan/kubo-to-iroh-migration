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

Looks up CID→iroh_hash in MongoDB (cid_iroh_mappings collection).
Reads blobs from /data/iroh/blobs/data/<hash>.data
"""
import os
import sys
import time
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

    Handles three cases:
    1. CIDv0/v1 (e.g., Qm...) → look up in cid_iroh_mappings.cid or _id
    2. Iroh hash (64 hex chars) passed directly → look up in cid_iroh_mappings.iroh_hash
    3. Cached result from previous lookup
    """
    if cid in _cid_cache:
        CACHE_HITS.labels(result="hit").inc()
        return _cid_cache[cid]
    # Fallback to DB
    doc = await mappings.find_one({"$or": [{"cid": cid}, {"_id": cid}]})
    if doc and doc.get("iroh_hash"):
        _cid_cache[cid] = doc["iroh_hash"]
        CACHE_HITS.labels(result="miss").inc()
        return doc["iroh_hash"]
    # If the input looks like an iroh hash (64 hex chars), try looking it up
    # directly in the iroh_hash field — the caller may be using iroh hashes
    # as content identifiers (post-migration content has no CIDv0)
    if len(cid) == 64 and all(c in "0123456789abcdef" for c in cid.lower()):
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
        {"_id": cid},
        {"$set": {
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
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else total_size - 1
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
    # For /add, the CID is the Iroh hash hex (we don't compute IPFS CIDs)
    # But existing services expect a Qm... CID. We store the hash as the CID.
    cid = hash_hex
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
    cid = hash_hex
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
    await mappings.update_one({"_id": cid}, {"$set": {"pinned": True}}, upsert=True)
    return web.json_response({"Pins": [cid]})


async def api_pin_ls(request: web.Request) -> web.Response:
    """POST /api/v0/pin/ls — list pinned CIDs."""
    pinned = {}
    async for doc in mappings.find({"pinned": True}, {"_id": 1}):
        cid = doc.get("_id")
        if isinstance(cid, str):
            pinned[cid] = {"Type": "recursive"}
    return web.json_response({"Keys": pinned})


async def api_pin_rm(request: web.Request) -> web.Response:
    """POST /api/v0/pin/rm?arg=<CID> — unmark pinned."""
    cid = request.query.get("arg", "")
    if not cid:
        raise web.HTTPBadRequest(text="Missing 'arg' parameter")
    await mappings.update_one({"_id": cid}, {"$set": {"pinned": False}})
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
    try:
        num_objects = len(list(BLOBS_DIR.glob("*.data")))
    except Exception:
        num_objects = 0
    return web.json_response({
        "RepoSize": 0,
        "StorageMax": 0,
        "NumObjects": str(num_objects),
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
