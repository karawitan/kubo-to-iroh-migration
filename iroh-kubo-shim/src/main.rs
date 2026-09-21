//! iroh-kubo-shim: Kubo-compatible HTTP API + IPFS gateway over an Iroh FsStore.
//!
//! Routes:
//!   GET  /ipfs/{cid}[/{path}]   — serve blob, HTTP Range (206) supported
//!   POST /api/v0/cat            — blob bytes
//!   POST /api/v0/add            — store blob, returns CIDv1 (bafk…)
//!   POST /api/v0/pin/{add,ls,rm}— pinned flag in MongoDB
//!   POST /api/v0/block/{stat,get}, /api/v0/ls, /api/v0/object/stat, /api/v0/dag/stat
//!   POST /api/v0/{version,id,repo/stat,stats/bw,swarm/peers,bitswap/stat,resolve}
//!   GET  /health, /metrics
//!
//! CID resolution: CIDv1 with blake3 multihash (bafk…) decodes natively;
//! CIDv0 (Qm…) resolves via MongoDB cid_iroh_mappings; raw 64-hex hashes are
//! accepted when the blob exists on disk.
//!
//! Blob writes are real FsStore entries: .data + .obao4 outboard + a
//! postcard-serialized EntryState record in the redb `blobs-0` table, so a
//! real iroh node can serve what this shim adds.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use axum::{
    Json, Router,
    body::Body,
    extract::{MatchedPath, Multipart, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::CreateOutboard;
use bao_tree::BlockSize;
use futures::StreamExt;
use iroh_base::hash::Hash as IrohHash;
use mongodb::bson::{doc, Document};
use prometheus::{HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
const CHUNK: u64 = 1024 * 1024;

// ── FsStore schema (mirrors iroh-blobs 0.28, see offline-migrator) ──────────

#[derive(Debug)]
struct EntryStateBytes;
impl redb::Value for EntryStateBytes {
    type SelfType<'a> = Vec<u8>;
    type AsBytes<'a> = Vec<u8>;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        data.to_vec()
    }
    fn as_bytes<'a, 'b: 'a>(v: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        v.clone()
    }
    fn type_name() -> redb::TypeName {
        redb::TypeName::new("EntryState")
    }
}
const BLOBS_TABLE: redb::TableDefinition<IrohHash, EntryStateBytes> =
    redb::TableDefinition::new("blobs-0");

#[derive(Serialize, Deserialize)]
enum EntryState {
    Complete {
        data_location: DataLocation,
        outboard_location: OutboardLocation,
    },
    #[allow(dead_code)]
    Partial {
        size: Option<u64>,
    },
}
#[derive(Serialize, Deserialize)]
enum DataLocation {
    Inline(()),
    Owned(u64),
    #[allow(dead_code)]
    External(Vec<PathBuf>, u64),
}
#[derive(Serialize, Deserialize)]
enum OutboardLocation {
    Inline(()),
    Owned,
    NotNeeded,
}

// ── CIDv1 (bafk…) ↔ blake3 ──────────────────────────────────────────────────

const BLAKE3_MULTICODEC: u64 = 0x1e;
const RAW_CODEC: u64 = 0x55;

fn decode_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if pos >= data.len() {
            return None;
        }
        let b = data[pos];
        pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

fn base32_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut result = String::new();
    let mut buffer: u64 = 0;
    let mut bits = 0;
    for &b in data {
        buffer = (buffer << 8) | (b as u64);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            result.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        result.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    result
}

fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut result = Vec::new();
    let mut buffer: u64 = 0;
    let mut bits = 0;
    for c in s.chars() {
        let idx = ALPHABET
            .iter()
            .position(|&a| a as char == c.to_ascii_lowercase())?;
        buffer = (buffer << 5) | (idx as u64);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            result.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Some(result)
}

/// blake3 hex → CIDv1 (raw codec + blake3 multihash): bafkr4i…
fn hash_hex_to_cidv1(hash_hex: &str) -> Option<String> {
    let digest = hex::decode(hash_hex).ok()?;
    if digest.len() != 32 {
        return None;
    }
    let mut cid = vec![0x01, RAW_CODEC as u8, BLAKE3_MULTICODEC as u8, 0x20];
    cid.extend_from_slice(&digest);
    Some(format!("b{}", base32_encode(&cid)))
}

/// CIDv1 carrying a blake3 multihash → hash hex. Codec field is ignored:
/// any CIDv1 whose multihash is blake3-256 maps to the same blob.
fn cidv1_to_hash(cid: &str) -> Option<String> {
    let body = cid.strip_prefix('b')?;
    let raw = base32_decode(body)?;
    let (version, pos) = decode_varint(&raw, 0)?;
    if version != 0x01 {
        return None;
    }
    let (_, pos) = decode_varint(&raw, pos)?; // codec
    let (mh_code, pos) = decode_varint(&raw, pos)?;
    if mh_code != BLAKE3_MULTICODEC {
        return None;
    }
    let (mh_len, pos) = decode_varint(&raw, pos)?;
    if mh_len != 0x20 || raw.len() - pos != 0x20 {
        return None;
    }
    Some(hex::encode(&raw[pos..]))
}

// ── State ───────────────────────────────────────────────────────────────────

struct Metrics {
    requests: IntCounterVec,
    bytes_served: prometheus::IntCounterVec,
    latency: HistogramVec,
    registry: Registry,
}

struct AppState {
    mappings: mongodb::Collection<Document>,
    blobs_dir: PathBuf,
    db: redb::Database,
    cache: RwLock<HashMap<String, String>>,
    metrics: Metrics,
}

fn metric(st: &AppState, endpoint: &str, status: u16) {
    st.metrics
        .requests
        .with_label_values(&[endpoint, &status.to_string()])
        .inc();
}

// ── CID → hash resolution ───────────────────────────────────────────────────

async fn lookup_iroh_hash(st: &AppState, cid: &str) -> Option<String> {
    if let Some(h) = st.cache.read().unwrap().get(cid) {
        return Some(h.clone());
    }
    // CIDv1 bafk… decodes deterministically — no mapping needed
    if let Some(h) = cidv1_to_hash(cid) {
        st.cache.write().unwrap().insert(cid.to_string(), h.clone());
        return Some(h);
    }
    // MongoDB: legacy CIDv0/v1 mappings
    let doc = st
        .mappings
        .find_one(doc! { "$or": [{"cid": cid}, {"_id": cid}] })
        .await
        .ok()??;
    if let Some(h) = doc.get_str("iroh_hash").ok() {
        let h = h.to_string();
        st.cache.write().unwrap().insert(cid.to_string(), h.clone());
        return Some(h);
    }
    // Raw 64-hex hash: accept when the blob exists, else try reverse mapping
    if cid.len() == 64 && cid.chars().all(|c| c.is_ascii_hexdigit()) {
        let h = cid.to_lowercase();
        if st.blobs_dir.join(format!("{h}.data")).exists() {
            st.cache.write().unwrap().insert(cid.to_string(), h.clone());
            return Some(h);
        }
        if let Ok(Some(doc)) = st
            .mappings
            .find_one(doc! { "iroh_hash": cid })
            .await
        {
            if let Ok(h) = doc.get_str("iroh_hash") {
                let h = h.to_string();
                st.cache.write().unwrap().insert(cid.to_string(), h.clone());
                return Some(h);
            }
        }
    }
    None
}

async fn store_mapping(st: &AppState, cid: &str, iroh_hash: &str, size: u64) {
    st.cache
        .write()
        .unwrap()
        .insert(cid.to_string(), iroh_hash.to_string());
    let _ = st
        .mappings
        .update_one(
            doc! { "_id": cid },
            doc! { "$set": {
                "iroh_hash": iroh_hash,
                "size": size as i64,
                "migrated_at": mongodb::bson::DateTime::now(),
                "pinned": true,
            }},
        )
        .upsert(true)
        .await;
}

// ── Blob store ──────────────────────────────────────────────────────────────

/// Write a blob as a real FsStore entry: .data + .obao4 + redb EntryState.
/// Returns (hash_bytes, hash_hex).
fn write_blob(st: &AppState, data: &[u8]) -> Result<([u8; 32], String)> {
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(
        std::io::Cursor::new(data),
        data.len() as u64,
        IROH_BLOCK_SIZE,
    )?;
    let hash_bytes: [u8; 32] = ob.root.into();
    let hash_hex = hex::encode(hash_bytes);

    std::fs::write(st.blobs_dir.join(format!("{hash_hex}.data")), data)?;
    if !ob.data.is_empty() {
        std::fs::write(st.blobs_dir.join(format!("{hash_hex}.obao4")), &ob.data)?;
    }

    let entry = EntryState::Complete {
        data_location: DataLocation::Owned(data.len() as u64),
        outboard_location: OutboardLocation::Owned,
    };
    let entry_bytes = postcard::to_allocvec(&entry)?;
    let txn = st.db.begin_write()?;
    {
        let mut table = txn.open_table(BLOBS_TABLE)?;
        table.insert(IrohHash::from_bytes(hash_bytes), entry_bytes)?;
    }
    txn.commit()?;
    Ok((hash_bytes, hash_hex))
}

fn blob_path(st: &AppState, hash_hex: &str) -> Option<PathBuf> {
    let p = st.blobs_dir.join(format!("{hash_hex}.data"));
    p.exists().then_some(p)
}

/// Stream `len` bytes of `file` starting at `start` as a response Body.
async fn file_body(path: &std::path::Path, start: u64, len: u64) -> std::io::Result<Body> {
    let mut file = tokio::fs::File::open(path).await?;
    if start > 0 {
        file.seek(std::io::SeekFrom::Start(start)).await?;
    }
    let stream = futures::stream::try_unfold((file, len), |(mut f, remaining)| async move {
        if remaining == 0 {
            return Ok(None);
        }
        let mut buf = vec![0u8; remaining.min(CHUNK) as usize];
        let n = f.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.truncate(n);
        Ok::<_, std::io::Error>(Some((buf, (f, remaining - n as u64))))
    });
    Ok(Body::from_stream(stream))
}

// ── Handlers ────────────────────────────────────────────────────────────────

async fn gateway_ipfs(
    Path(cid): Path<String>,
    headers: HeaderMap,
    State(st): State<Arc<AppState>>,
) -> Response {
    serve_gateway(cid, headers, st).await
}

async fn gateway_ipfs_path(
    Path((cid, _rest)): Path<(String, String)>,
    headers: HeaderMap,
    State(st): State<Arc<AppState>>,
) -> Response {
    serve_gateway(cid, headers, st).await
}

async fn serve_gateway(cid: String, headers: HeaderMap, st: Arc<AppState>) -> Response {
    let Some(hash) = lookup_iroh_hash(&st, &cid).await else {
        metric(&st, "gateway", 404);
        return (StatusCode::NOT_FOUND, format!("CID not found: {cid}")).into_response();
    };
    let Some(path) = blob_path(&st, &hash) else {
        metric(&st, "gateway", 404);
        return (StatusCode::NOT_FOUND, format!("Blob not found: {hash}")).into_response();
    };
    let total = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    if let Some(range) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(spec) = range.strip_prefix("bytes=") {
            let spec = spec.split(',').next().unwrap_or("").trim();
            if let Some((s, e)) = spec.split_once('-') {
                let start: u64 = s.parse().unwrap_or(0);
                let mut end: u64 = e.parse().unwrap_or(total.saturating_sub(1));
                if end >= total {
                    end = total.saturating_sub(1);
                }
                if start > end || start >= total {
                    metric(&st, "gateway", 416);
                    return Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header(header::CONTENT_RANGE, format!("bytes */{total}"))
                        .body(Body::empty())
                        .unwrap();
                }
                let len = end - start + 1;
                st.metrics
                    .bytes_served
                    .with_label_values(&["gateway"])
                    .inc_by(len);
                metric(&st, "gateway", 206);
                return match file_body(&path, start, len).await {
                    Ok(body) => Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{total}"))
                        .header(header::ACCEPT_RANGES, "bytes")
                        .header(header::CONTENT_LENGTH, len)
                        .header(header::CONTENT_TYPE, "application/octet-stream")
                        .body(body)
                        .unwrap(),
                    Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                };
            }
        }
    }

    st.metrics
        .bytes_served
        .with_label_values(&["gateway"])
        .inc_by(total);
    metric(&st, "gateway", 200);
    match file_body(&path, 0, total).await {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(header::ACCEPT_RANGES, "bytes")
            .header(header::CONTENT_LENGTH, total)
            .body(body)
            .unwrap(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct ArgQuery {
    arg: Option<String>,
    #[serde(rename = "only-hash")]
    only_hash: Option<String>,
}

async fn api_cat(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    let Some(hash) = lookup_iroh_hash(&st, &cid).await else {
        metric(&st, "cat", 404);
        return (StatusCode::NOT_FOUND, format!("CID not found: {cid}")).into_response();
    };
    let Some(path) = blob_path(&st, &hash) else {
        metric(&st, "cat", 404);
        return (StatusCode::NOT_FOUND, format!("Blob not found: {hash}")).into_response();
    };
    match tokio::fs::read(&path).await {
        Ok(data) => {
            st.metrics
                .bytes_served
                .with_label_values(&["cat"])
                .inc_by(data.len() as u64);
            metric(&st, "cat", 200);
            data.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn api_add(
    Query(q): Query<ArgQuery>,
    State(st): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Response {
    let Ok(Some(field)) = multipart.next_field().await else {
        return (StatusCode::BAD_REQUEST, "No file in upload").into_response();
    };
    let name = field.file_name().unwrap_or("file").to_string();
    let Ok(data) = field.bytes().await else {
        return (StatusCode::BAD_REQUEST, "Failed to read upload").into_response();
    };
    let size = data.len() as u64;
    let only_hash = q.only_hash.as_deref() == Some("true");

    let st2 = st.clone();
    let res = tokio::task::spawn_blocking(move || {
        if only_hash {
            // Hash without storing — still the bao-tree root iroh would use
            let ob = PreOrderOutboard::<Vec<u8>>::create_sized(
                std::io::Cursor::new(&data[..]),
                size,
                IROH_BLOCK_SIZE,
            )?;
            let hb: [u8; 32] = ob.root.into();
            Ok::<_, anyhow::Error>((hb, hex::encode(hb)))
        } else {
            write_blob(&st2, &data)
        }
    })
    .await;

    match res {
        Ok(Ok((_hash_bytes, hash_hex))) => {
            let cid = hash_hex_to_cidv1(&hash_hex).unwrap_or(hash_hex.clone());
            if !only_hash {
                store_mapping(&st, &cid, &hash_hex, size).await;
            }
            metric(&st, "add", 200);
            Json(serde_json::json!({
                "Name": name,
                "Hash": cid,
                "Size": size.to_string(),
            }))
            .into_response()
        }
        _ => {
            metric(&st, "add", 500);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn api_pin_add(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    let _ = st
        .mappings
        .update_one(
            doc! { "_id": &cid },
            doc! { "$set": { "pinned": true } },
        )
        .upsert(true)
        .await;
    Json(serde_json::json!({ "Pins": [cid] })).into_response()
}

async fn api_pin_rm(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    let _ = st
        .mappings
        .update_one(
            doc! { "_id": &cid },
            doc! { "$set": { "pinned": false } },
        )
        .await;
    Json(serde_json::json!({ "Pins": [cid] })).into_response()
}

async fn api_pin_ls(State(st): State<Arc<AppState>>) -> Response {
    let mut keys = serde_json::Map::new();
    if let Ok(mut cursor) = st
        .mappings
        .find(doc! { "pinned": true })
        .await
    {
        while let Some(Ok(doc)) = cursor.next().await {
            if let Ok(cid) = doc.get_str("_id") {
                keys.insert(cid.to_string(), serde_json::json!({ "Type": "recursive" }));
            }
        }
    }
    Json(serde_json::json!({ "Keys": keys })).into_response()
}

async fn blob_size(st: &AppState, cid: &str) -> Option<u64> {
    let hash = lookup_iroh_hash(st, cid).await?;
    let path = blob_path(st, &hash)?;
    std::fs::metadata(path).ok().map(|m| m.len())
}

async fn api_block_stat(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    match blob_size(&st, &cid).await {
        Some(size) => Json(serde_json::json!({ "Key": cid, "Size": size.to_string() }))
            .into_response(),
        None => (StatusCode::NOT_FOUND, format!("CID not found: {cid}")).into_response(),
    }
}

async fn api_block_get(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    api_cat(Query(q), State(st)).await
}

async fn api_ls(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    match blob_size(&st, &cid).await {
        Some(size) => Json(serde_json::json!({
            "Objects": [{ "Hash": cid, "Size": size, "Type": "file", "Links": [] }]
        }))
        .into_response(),
        None => (StatusCode::NOT_FOUND, format!("CID not found: {cid}")).into_response(),
    }
}

async fn api_object_stat(Query(q): Query<ArgQuery>, State(st): State<Arc<AppState>>) -> Response {
    let Some(cid) = q.arg else {
        return (StatusCode::BAD_REQUEST, "Missing 'arg' parameter").into_response();
    };
    match blob_size(&st, &cid).await {
        Some(size) => Json(serde_json::json!({
            "Hash": cid, "NumLinks": 0, "BlockSize": size,
            "LinksSize": 0, "DataSize": size, "CumulativeSize": size,
        }))
        .into_response(),
        None => (StatusCode::NOT_FOUND, format!("CID not found: {cid}")).into_response(),
    }
}

async fn api_version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "Version": "0.1.0-iroh-kubo-shim",
        "Commit": "iroh",
        "System": std::env::consts::ARCH,
        "Golang": "iroh",
    }))
}

async fn api_id() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ID": "iroh-kubo-shim", "Addresses": [], "Protocols": [] }))
}

async fn api_repo_stat(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let num = std::fs::read_dir(&st.blobs_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "data").unwrap_or(false))
                .count()
        })
        .unwrap_or(0);
    Json(serde_json::json!({
        "RepoSize": 0, "StorageMax": 0, "NumObjects": num.to_string(),
    }))
}

async fn api_stats_bw() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "TotalIn": 0, "TotalOut": 0, "RateIn": 0, "RateOut": 0 }))
}

async fn api_swarm_peers() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "Peers": [] }))
}

async fn api_bitswap_stat() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "Wantlist": [], "BlocksReceived": 0, "BlocksSent": 0, "Peers": [],
    }))
}

async fn api_resolve(Query(q): Query<ArgQuery>) -> Json<serde_json::Value> {
    let cid = q.arg.unwrap_or_default();
    Json(serde_json::json!({ "Path": format!("/ipfs/{cid}") }))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true, "service": "iroh-kubo-shim" }))
}

async fn track_latency(State(st): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let endpoint = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let start = std::time::Instant::now();
    let resp = next.run(req).await;
    st.metrics
        .latency
        .with_label_values(&[&endpoint])
        .observe(start.elapsed().as_secs_f64());
    resp
}

async fn metrics(State(st): State<Arc<AppState>>) -> Response {
    let encoder = TextEncoder::new();
    let families = st.metrics.registry.gather();
    match encoder.encode_to_string(&families) {
        Ok(s) => ([(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)], s).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ── Startup ─────────────────────────────────────────────────────────────────

async fn load_cache(st: &AppState) -> Result<()> {
    let mut cursor = st
        .mappings
        .find(doc! { "iroh_hash": { "$exists": true } })
        .projection(doc! { "_id": 1, "iroh_hash": 1, "cid": 1 })
        .await?;
    let mut cache = st.cache.write().unwrap();
    while let Some(doc) = cursor.next().await {
        let doc = doc?;
        let cid = doc
            .get_str("cid")
            .or_else(|_| doc.get_str("_id"))
            .ok()
            .map(|s| s.to_string());
        if let (Some(cid), Ok(hash)) = (cid, doc.get_str("iroh_hash")) {
            cache.insert(cid, hash.to_string());
        }
    }
    tracing::info!(count = cache.len(), "loaded CID→iroh_hash mappings");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_kubo_shim=info".into()),
        )
        .init();

    let iroh_data_dir = std::env::var("IROH_DATA_DIR").unwrap_or_else(|_| "/data/iroh".into());
    let blobs_dir = PathBuf::from(&iroh_data_dir).join("blobs").join("data");
    let db_path = PathBuf::from(&iroh_data_dir).join("blobs").join("blobs.db");
    let mongo_url = std::env::var("MONGO_URL").unwrap_or_else(|_| "mongodb://mongo:27017/hub".into());
    let db_name = std::env::var("DB_NAME").unwrap_or_else(|_| "hub".into());
    let collection = std::env::var("CID_MAPPINGS").unwrap_or_else(|_| "cid_iroh_mappings".into());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5001);

    let mongo = mongodb::Client::with_uri_str(&mongo_url).await?;
    let mappings = mongo.database(&db_name).collection::<Document>(&collection);
    let db = redb::Database::open(&db_path)?;

    let registry = Registry::new();
    let requests = IntCounterVec::new(
        Opts::new("iroh_shim_requests_total", "Total requests by endpoint and status"),
        &["endpoint", "status"],
    )?;
    let bytes_served = IntCounterVec::new(
        Opts::new("iroh_shim_bytes_served_total", "Total bytes served by endpoint"),
        &["endpoint"],
    )?;
    let latency = HistogramVec::new(
        prometheus::HistogramOpts::new(
            "iroh_shim_request_duration_seconds",
            "Request latency by endpoint",
        ),
        &["endpoint"],
    )?;
    registry.register(Box::new(requests.clone()))?;
    registry.register(Box::new(bytes_served.clone()))?;
    registry.register(Box::new(latency.clone()))?;

    let st = Arc::new(AppState {
        mappings,
        blobs_dir,
        db,
        cache: RwLock::new(HashMap::new()),
        metrics: Metrics {
            requests,
            bytes_served,
            latency,
            registry,
        },
    });

    load_cache(&st).await?;

    let app = Router::new()
        .route("/ipfs/{cid}", get(gateway_ipfs))
        .route("/ipfs/{cid}/{*rest}", get(gateway_ipfs_path))
        .route("/api/v0/cat", post(api_cat))
        .route("/api/v0/add", post(api_add))
        .route("/api/v0/pin/add", post(api_pin_add))
        .route("/api/v0/pin/ls", post(api_pin_ls))
        .route("/api/v0/pin/rm", post(api_pin_rm))
        .route("/api/v0/block/stat", post(api_block_stat))
        .route("/api/v0/block/get", post(api_block_get))
        .route("/api/v0/ls", post(api_ls))
        .route("/api/v0/version", post(api_version))
        .route("/api/v0/id", post(api_id))
        .route("/api/v0/repo/stat", post(api_repo_stat))
        .route("/api/v0/stats/bw", post(api_stats_bw))
        .route("/api/v0/swarm/peers", post(api_swarm_peers))
        .route("/api/v0/bitswap/stat", post(api_bitswap_stat))
        .route("/api/v0/resolve", post(api_resolve))
        .route("/api/v0/object/stat", post(api_object_stat))
        .route("/api/v0/dag/stat", post(api_object_stat))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(st.clone(), track_latency))
        .with_state(st);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(%addr, "iroh-kubo-shim listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidv1_roundtrip() {
        let h = "746a4c34de0949197829cb83f16dca6922c4f874e401b0d4daec4d5b1a454796";
        let cid = hash_hex_to_cidv1(h).unwrap();
        assert!(cid.starts_with("bafkr4i"), "{cid}");
        assert_eq!(cidv1_to_hash(&cid).as_deref(), Some(h));
    }

    #[test]
    fn cidv1_decodes_real_iroh_hash() {
        // CID from iroh-gateway README — note its final base32 char has
        // non-canonical pad bits, so compare via decode, not re-encode.
        let cid = "bafkr4ibwueokpaxz3x6fd2tvsdo3uqixzimulg536abicpqmqtt3itaodt";
        let h = cidv1_to_hash(cid).unwrap();
        assert_eq!(h.len(), 64);
        // canonical re-encode decodes back to the same hash
        assert_eq!(
            cidv1_to_hash(&hash_hex_to_cidv1(&h).unwrap()).as_deref(),
            Some(h.as_str())
        );
    }

    #[test]
    fn cidv1_rejects_non_blake3() {
        // CIDv0 and CIDv1 sha2-256/dag-pb must fall through to the mapping lookup
        assert!(cidv1_to_hash("QmNLexjuJXxxnsAarYUGejf6gacAkmcvwbs4RWtgpxq932").is_none());
        assert!(
            cidv1_to_hash("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
                .is_none()
        );
    }
}
