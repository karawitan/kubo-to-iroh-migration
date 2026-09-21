use futures::StreamExt;
use anyhow::{anyhow, Result};
use bao_tree::io::sync::CreateOutboard;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::BlockSize;
use chrono::Utc;
use iroh_base::hash::Hash as IrohHash;
use redb::{TableDefinition, TypeName, Value, ReadableTable};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
const BATCH_SIZE: usize = 200;

#[derive(Debug)]
struct EntryStateBytes;
impl Value for EntryStateBytes {
    type SelfType<'a> = Vec<u8>;
    type AsBytes<'a> = Vec<u8>;
    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a { data.to_vec() }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'a, Self: 'b { value.clone() }
    fn type_name() -> TypeName { TypeName::new("EntryState") }
}
const BLOBS_TABLE: TableDefinition<IrohHash, EntryStateBytes> = TableDefinition::new("blobs-0");

fn ts() -> String { Utc::now().format("%Y-%m-%d %H:%M:%S").to_string() }

#[derive(Serialize, Deserialize)]
enum EntryState {
    Complete { data_location: DataLocation, outboard_location: OutboardLocation },
    #[allow(dead_code)] Partial { size: Option<u64> },
}
#[derive(Serialize, Deserialize)]
enum DataLocation {
    Inline(()),
    Owned(u64),
    #[allow(dead_code)] External(Vec<PathBuf>, u64),
}
#[derive(Serialize, Deserialize)]
enum OutboardLocation {
    Inline(()),
    Owned,
    NotNeeded,
}

fn compute_hash(data: &[u8]) -> Result<([u8; 32], String, Vec<u8>)> {
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(
        std::io::Cursor::new(data), data.len() as u64, IROH_BLOCK_SIZE,
    )?;
    let hash_bytes: [u8; 32] = ob.root.into();
    let hash_hex = hex::encode(hash_bytes);
    Ok((hash_bytes, hash_hex, ob.data))
}

fn write_blob(data_dir: &std::path::Path, hash_hex: &str, data: &[u8], outboard: &[u8]) -> Result<()> {
    let data_path = data_dir.join(format!("{}.data", hash_hex));
    std::fs::write(&data_path, data)?;
    if !outboard.is_empty() {
        let ob_path = data_dir.join(format!("{}.obao4", hash_hex));
        std::fs::write(&ob_path, outboard)?;
    }
    Ok(())
}

fn process_one(data_dir: &std::path::Path, data: &[u8]) -> Result<([u8; 32], String, Vec<u8>)> {
    let (hash_bytes, hash_hex, outboard) = compute_hash(data)?;
    write_blob(data_dir, &hash_hex, data, &outboard)?;
    Ok((hash_bytes, hash_hex, outboard))
}

fn batch_commit_db(db: &redb::Database, blobs: &[(IrohHash, Vec<u8>)]) -> Result<()> {
    let txn = db.begin_write()?;
    {
        let mut table = txn.open_table(BLOBS_TABLE)?;
        for (hash, entry_bytes) in blobs {
            table.insert(*hash, entry_bytes.clone())?;
        }
    }
    txn.commit()?;
    Ok(())
}

// ── Direct block reading from disk (bypasses Kubo API) ───────────────

/// Decode a varint from a byte slice. Returns (value, new_pos).
fn decode_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if pos >= data.len() { return None; }
        let b = data[pos];
        pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 { break; }
        shift += 7;
        if shift >= 64 { return None; }
    }
    Some((result, pos))
}

/// DAG-PB Link: { Hash (bytes), Name (string), Tsize (u64) }
struct PbLink {
    hash: Vec<u8>,
    name: String,
    tsize: u64,
}

/// DAG-PB Node: { Links ([]Link), Data (bytes) }
struct PbNode {
    links: Vec<PbLink>,
    data: Vec<u8>,
}

/// UnixFS: { Type (enum), Data (bytes), filesize (u64), blocksizes ([]u64) }
struct UnixFS {
    ftype: u8,
    data: Vec<u8>,
    filesize: u64,
    blocksizes: Vec<u64>,
}

fn decode_pbnode(data: &[u8]) -> Option<PbNode> {
    let mut pos = 0;
    let mut links = Vec::new();
    let mut node_data = Vec::new();
    while pos < data.len() {
        let (key, p) = decode_varint(data, pos)?;
        pos = p;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u32;
        match wire_type {
            2 => {
                let (len, p) = decode_varint(data, pos)?;
                pos = p;
                if pos + len as usize > data.len() { return None; }
                let value = &data[pos..pos + len as usize];
                pos += len as usize;
                match field_num {
                    1 => {
                        // Links (repeated)
                        if let Some(link) = decode_pblink(value) {
                            links.push(link);
                        }
                    }
                    2 => {
                        // Data
                        node_data = value.to_vec();
                    }
                    _ => {}
                }
            }
            0 => {
                let (_, p) = decode_varint(data, pos)?;
                pos = p;
            }
            _ => { break; }
        }
    }
    Some(PbNode { links, data: node_data })
}

fn decode_pblink(data: &[u8]) -> Option<PbLink> {
    let mut pos = 0;
    let mut hash = Vec::new();
    let mut name = String::new();
    let mut tsize = 0u64;
    while pos < data.len() {
        let (key, p) = decode_varint(data, pos)?;
        pos = p;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u32;
        match wire_type {
            2 => {
                let (len, p) = decode_varint(data, pos)?;
                pos = p;
                if pos + len as usize > data.len() { return None; }
                let value = &data[pos..pos + len as usize];
                pos += len as usize;
                match field_num {
                    1 => hash = value.to_vec(),  // Hash
                    2 => name = String::from_utf8_lossy(value).into_owned(),  // Name
                    _ => {}
                }
            }
            0 => {
                let (val, p) = decode_varint(data, pos)?;
                pos = p;
                if field_num == 3 { tsize = val; }  // Tsize
            }
            _ => { break; }
        }
    }
    Some(PbLink { hash, name, tsize })
}

fn decode_unixfs(data: &[u8]) -> Option<UnixFS> {
    let mut pos = 0;
    let mut ftype = 0u8;
    let mut fs_data = Vec::new();
    let mut filesize = 0u64;
    let mut blocksizes = Vec::new();
    while pos < data.len() {
        let (key, p) = decode_varint(data, pos)?;
        pos = p;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u32;
        match wire_type {
            2 => {
                let (len, p) = decode_varint(data, pos)?;
                pos = p;
                if pos + len as usize > data.len() { return None; }
                let value = &data[pos..pos + len as usize];
                pos += len as usize;
                if field_num == 2 {  // Data
                    fs_data = value.to_vec();
                }
            }
            0 => {
                let (val, p) = decode_varint(data, pos)?;
                pos = p;
                match field_num {
                    1 => ftype = val as u8,  // Type
                    3 => filesize = val,     // filesize
                    4 => blocksizes.push(val),  // blocksizes
                    _ => {}
                }
            }
            _ => { break; }
        }
    }
    Some(UnixFS { ftype, data: fs_data, filesize, blocksizes })
}

/// Convert a CID string to its Kubo block file path on disk.
/// Kubo stores blocks in sharded dirs: blocks/<last-2-chars-of-base32>/<full-base32>.data
/// The base32 encoding is of the *multihash* (not the CIDv1).
fn cid_to_block_path(blocks_dir: &std::path::Path, cid: &str) -> Option<PathBuf> {
    let b32 = if cid.starts_with("Qm") {
        // CIDv0: base58 decode to get multihash, then base32 encode
        let raw = bs58::decode(cid).into_vec().ok()?;
        base32_encode(&raw)
    } else if cid.starts_with("bafy") || cid.starts_with("bafk") || cid.starts_with("bafz") {
        // CIDv1 base32: strip 'b' prefix, decode to get CIDv1 bytes, extract multihash
        let b32_input = &cid[1..];
        let decoded = base32_decode(b32_input)?;
        // CIDv1 = version(1) + codec(1) + multihash
        if decoded.len() < 2 { return None; }
        let multihash = &decoded[2..];
        base32_encode(multihash)
    } else {
        return None;
    };
    // Sharding: directory is chars at position [-3:-1] of the base32 name
    if b32.len() < 3 { return None; }
    let dir_chars = &b32[b32.len() - 3..b32.len() - 1];
    Some(blocks_dir.join(dir_chars).join(format!("{}.data", b32)))
}

/// Base32 encode (uppercase, no padding) — matches Kubo's block naming.
fn base32_encode(data: &[u8]) -> String {
    // Use a simple implementation
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut result = String::new();
    let mut buffer: u64 = 0;
    let mut bits = 0;
    for &b in data {
        buffer = (buffer << 8) | (b as u64);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1f) as usize;
            result.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1f) as usize;
        result.push(ALPHABET[idx] as char);
    }
    result
}

/// Base32 decode (case-insensitive, no padding).
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut result = Vec::new();
    let mut buffer: u64 = 0;
    let mut bits = 0;
    for c in s.chars() {
        let upper = c.to_ascii_uppercase();
        let idx = ALPHABET.iter().position(|&a| a as char == upper)?;
        buffer = (buffer << 5) | (idx as u64);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            result.push((buffer >> bits) as u8 & 0xff);
        }
    }
    Some(result)
}

/// Convert raw hash bytes (from a DAG-PB Link) back to a CID string.
/// For CIDv0 (SHA2-256, 32 bytes): base58 encode the multihash.
/// For CIDv1: we need version + codec + multihash. The link hash in DAG-PB
/// is a full CID (with version prefix), not just a multihash.
fn hash_to_cid(hash_bytes: &[u8]) -> Option<String> {
    if hash_bytes.len() == 34 && hash_bytes[0] == 0x12 && hash_bytes[1] == 0x20 {
        // CIDv0: multihash (sha2-256, 32 bytes)
        Some(bs58::encode(hash_bytes).into_string())
    } else if hash_bytes.len() >= 2 && hash_bytes[0] == 0x01 {
        // CIDv1: version + codec + multihash
        let b32 = base32_encode(hash_bytes).to_lowercase();
        Some(format!("b{}", b32))
    } else if hash_bytes.len() >= 2 && hash_bytes[0] == 0x00 {
        // CIDv0 with explicit version byte
        let mh = &hash_bytes[1..];
        Some(bs58::encode(mh).into_string())
    } else {
        // Assume it's a raw multihash (CIDv0 style)
        if hash_bytes.len() == 34 && hash_bytes[0] == 0x12 {
            Some(bs58::encode(hash_bytes).into_string())
        } else {
            None
        }
    }
}

/// Read a block file directly from disk.
fn read_block(blocks_dir: &std::path::Path, cid: &str) -> Option<Vec<u8>> {
    let path = cid_to_block_path(blocks_dir, cid)?;
    std::fs::read(&path).ok()
}

/// Block reader with Iroh fallback for missing child blocks.
/// When a child block is missing from Kubo's disk store (deleted in cleanup),
/// it looks up the CID in MongoDB mappings and reads the content from Iroh.
struct BlockReader {
    blocks_dir: std::path::PathBuf,
    blobs_dir: std::path::PathBuf,
    cid_to_iroh: std::sync::Arc<std::collections::HashMap<String, [u8; 32]>>,
}

impl BlockReader {
    /// Read a raw block from Kubo disk store only.
    fn read_block_from_disk(&self, cid: &str) -> Option<Vec<u8>> {
        read_block(&self.blocks_dir, cid)
    }

    /// Read already-assembled content from Iroh store.
    /// The .data file contains the full file content for that CID
    /// (as returned by kubo_cat during the original migration).
    /// This is NOT a raw DAG-PB block — it's the final content.
    fn read_content_from_iroh(&self, cid: &str) -> Option<Vec<u8>> {
        let hash_bytes = self.cid_to_iroh.get(cid)?;
        let hash_hex: String = hash_bytes.iter().map(|b| format!("{:02x}", b)).collect();
        let iroh_path = format!("{}/{}.data", self.blobs_dir.display(), hash_hex);
        std::fs::read(&iroh_path).ok()
    }

    /// Read full content for a CID by traversing its DAG.
    /// Tries Kubo disk first (raw block → parse DAG-PB).
    /// Falls back to Iroh (already-assembled content → use directly).
    fn read_cid_content(&self, cid: &str) -> Result<Vec<u8>> {
        // Try Kubo disk first: read raw block and parse DAG-PB
        if let Some(block_data) = self.read_block_from_disk(cid) {
            return self.parse_dag_pb(&block_data);
        }
        // Fallback: read already-assembled content from Iroh
        if let Some(content) = self.read_content_from_iroh(cid) {
            return Ok(content);
        }
        Err(anyhow!("Block not found on disk or Iroh: {}", cid))
    }

    /// Parse a raw DAG-PB block and extract file content.
    fn parse_dag_pb(&self, block_data: &[u8]) -> Result<Vec<u8>> {
        // Scan all top-level protobuf fields for sub-messages
        let mut all_child_hashes: Vec<Vec<u8>> = Vec::new();
        let mut file_content: Option<Vec<u8>> = None;
        let mut pos = 0;
        while pos < block_data.len() {
            let (key, p) = match decode_varint(block_data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = p;
            let wire_type = (key & 0x07) as u32;
            if wire_type == 2 {
                let (len, p) = match decode_varint(block_data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = p;
                let len = len as usize;
                if pos + len > block_data.len() { break; }
                let field_data = &block_data[pos..pos + len];
                pos += len;
                let (child_hashes, content) = scan_submessage(field_data);
                all_child_hashes.extend(child_hashes);
                if file_content.is_none() {
                    file_content = content;
                }
            } else if wire_type == 0 {
                let (_, p) = match decode_varint(block_data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = p;
            } else {
                break;
            }
        }

        // If we found inline file content, return it
        if let Some(content) = file_content {
            if !all_child_hashes.is_empty() {
                let mut result = content;
                for hash in &all_child_hashes {
                    let child_cid = hash_to_cid(hash)
                        .ok_or_else(|| anyhow!("Failed to convert child hash to CID"))?;
                    let child_data = self.read_cid_content(&child_cid)
                        .map_err(|_| anyhow!("Child block not found: {}", child_cid))?;
                    result.extend_from_slice(&child_data);
                }
                return Ok(result);
            }
            return Ok(content);
        }

        // If we found child hashes, fetch each child and concatenate
        if !all_child_hashes.is_empty() {
            let mut content = Vec::new();
            for hash in &all_child_hashes {
                let child_cid = hash_to_cid(hash)
                    .ok_or_else(|| anyhow!("Failed to convert child hash to CID"))?;
                let child_data = self.read_cid_content(&child_cid)
                    .map_err(|_| anyhow!("Child block not found: {}", child_cid))?;
                content.extend_from_slice(&child_data);
            }
            return Ok(content);
        }

        // If neither inline content nor child hashes, return raw block data
        // (might be a raw leaf)
        Ok(block_data.to_vec())
    }
}

/// Scan a protobuf sub-message for child hashes and file content.
/// Returns (child_hashes, file_content).
/// child_hashes: all 34-byte multihashes found (starting with 0x12 0x20)
/// file_content: the first non-empty bytes field that isn't a multihash
fn scan_submessage(data: &[u8]) -> (Vec<Vec<u8>>, Option<Vec<u8>>) {
    let mut child_hashes = Vec::new();
    let mut file_content: Option<Vec<u8>> = None;
    let mut pos = 0;
    while pos < data.len() {
        let (key, p) = match decode_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = p;
        let wire_type = (key & 0x07) as u32;
        if wire_type == 2 {
            // Length-delimited
            let (len, p) = match decode_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = p;
            let len = len as usize;
            if pos + len > data.len() { break; }
            let value = &data[pos..pos + len];
            pos += len;
            // Check if this is a 34-byte multihash (child hash)
            if value.len() == 34 && value[0] == 0x12 && value[1] == 0x20 {
                child_hashes.push(value.to_vec());
            } else if !value.is_empty() && file_content.is_none() && len > 2 {
                // This might be file content (not a tiny field)
                file_content = Some(value.to_vec());
            }
        } else if wire_type == 0 {
            // Varint — skip value
            let (_, p) = match decode_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = p;
        } else {
            break;
        }
    }
    (child_hashes, file_content)
}

/// Read full content for a CID by traversing its DAG directly from disk.
/// This bypasses the Kubo API entirely.
/// Handles go-ipld-prime's non-standard DAG-PB encoding where:
/// - Inline files: field 1 = UnixFS Data with Type=0, Data=content
/// - Files with children: field 2 = PBLink with Hash=child multihash
#[allow(dead_code)]
fn read_cid_content(blocks_dir: &std::path::Path, cid: &str) -> Result<Vec<u8>> {
    let block_data = read_block(blocks_dir, cid)
        .ok_or_else(|| anyhow!("Block not found on disk: {}", cid))?;

    // Scan all top-level protobuf fields for sub-messages
    let mut all_child_hashes: Vec<Vec<u8>> = Vec::new();
    let mut file_content: Option<Vec<u8>> = None;
    let mut pos = 0;
    while pos < block_data.len() {
        let (key, p) = match decode_varint(&block_data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = p;
        let wire_type = (key & 0x07) as u32;
        if wire_type == 2 {
            let (len, p) = match decode_varint(&block_data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = p;
            let len = len as usize;
            if pos + len > block_data.len() { break; }
            let field_data = &block_data[pos..pos + len];
            pos += len;
            // Try to parse this field as a sub-message
            let (child_hashes, content) = scan_submessage(field_data);
            all_child_hashes.extend(child_hashes);
            if file_content.is_none() {
                file_content = content;
            }
        } else if wire_type == 0 {
            let (_, p) = match decode_varint(&block_data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = p;
        } else {
            break;
        }
    }

    // If we found inline file content, return it
    if let Some(content) = file_content {
        if !all_child_hashes.is_empty() {
            // Both inline content and child hashes — concatenate
            let mut result = content;
            for hash in &all_child_hashes {
                let child_cid = hash_to_cid(hash)
                    .ok_or_else(|| anyhow!("Failed to convert child hash to CID"))?;
                match read_cid_content(blocks_dir, &child_cid) {
                    Ok(child_data) => result.extend_from_slice(&child_data),
                    Err(_) => {} // skip missing children
                }
            }
            return Ok(result);
        }
        return Ok(content);
    }

    // If we found child hashes, fetch each child and concatenate
    if !all_child_hashes.is_empty() {
        let mut content = Vec::new();
        for hash in &all_child_hashes {
            let child_cid = hash_to_cid(hash)
                .ok_or_else(|| anyhow!("Failed to convert child hash to CID"))?;
            let child_data = read_cid_content(blocks_dir, &child_cid)
                .or_else(|_| {
                    // If recursive parse fails, try reading raw block
                    read_block(blocks_dir, &child_cid)
                        .ok_or_else(|| anyhow!("Child block not found: {}", child_cid))
                })?;
            content.extend_from_slice(&child_data);
        }
        return Ok(content);
    }

    // If neither inline content nor child hashes, return raw block data
    // (might be a raw leaf)
    Ok(block_data)
}

fn kubo_cat(kubo_api: &str, cid: &str) -> Result<Vec<u8>> {
    let client = reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(300)).build()?;
    let resp = client.post(format!("{}/cat?arg={}", kubo_api, cid)).send()?;
    if !resp.status().is_success() {
        return Err(anyhow!("Kubo cat failed for {}: {}", cid, resp.status()));
    }
    Ok(resp.bytes()?.to_vec())
}

fn get_kubo_pins(kubo_api: &str) -> Result<Vec<String>> {
    let client = reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(300)).build()?;
    let resp = client.post(format!("{}/pin/ls?type=recursive&offline=true", kubo_api)).send()?;
    if !resp.status().is_success() {
        return Err(anyhow!("Kubo pin/ls failed: {}", resp.status()));
    }
    let body: serde_json::Value = resp.json()?;
    let keys = body["Keys"].as_object().ok_or_else(|| anyhow!("No Keys in pin/ls response"))?;
    Ok(keys.keys().cloned().collect())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let kubo_api = args.iter().find_map(|a| a.strip_prefix("--kubo-api=")).unwrap_or("http://127.0.0.1:5004/api/v0").to_string();
    let mongo_uri = args.iter().find_map(|a| a.strip_prefix("--mongo-uri=")).unwrap_or("mongodb://127.0.0.1:27017/hub").to_string();
    let iroh_data_dir = args.iter().find_map(|a| a.strip_prefix("--iroh-data-dir=")).unwrap_or("/data/iroh").to_string();
    let kubo_blocks_dir = args.iter().find_map(|a| a.strip_prefix("--kubo-blocks=")).unwrap_or("/data/ipfs/blocks").to_string();
    let workers: usize = args.iter().find_map(|a| a.strip_prefix("--workers=").and_then(|s| s.parse().ok())).unwrap_or(10);
    let skip_existing = args.iter().any(|a| a == "--skip-existing");
    let direct_read = args.iter().any(|a| a == "--direct");
    let mappings_only = args.iter().any(|a| a == "--mappings-only");

    let blobs_dir = format!("{}/blobs/data", iroh_data_dir);
    let db_path = format!("{}/blobs/blobs.db", iroh_data_dir);
    let data_dir = std::path::Path::new(&blobs_dir);
    let blocks_dir = std::path::Path::new(&kubo_blocks_dir);

    println!("{} Opening database at {}", ts(), db_path);
    let db = redb::Database::open(&db_path)?;
    let db = Arc::new(db);

    let rt = tokio::runtime::Runtime::new()?;
    let mongo_client = rt.block_on(async { mongodb::Client::with_uri_str(&mongo_uri).await })?;
    let collection = mongo_client.database("hub").collection::<mongodb::bson::Document>("cid_iroh_mappings");
    let collection = Arc::new(collection);
    let rt = Arc::new(rt);

    println!("{} Fetching pin list from Kubo...", ts());
    let cids = get_kubo_pins(&kubo_api)?;
    let total = cids.len();
    println!("{} Kubo has {} pinned CIDs", ts(), total);

    // Always load CID→iroh_hash mappings from MongoDB (used for Iroh fallback)
    println!("{} Loading CID→iroh_hash mappings from MongoDB...", ts());
    let coll = collection.clone();
    let rt2 = rt.clone();
    let cid_to_hash: std::collections::HashMap<String, [u8; 32]> = rt2.block_on(async move {
        let mut map = std::collections::HashMap::new();
        let cursor = coll.find(mongodb::bson::doc! { "iroh_hash": { "$exists": true } }).await;
        if let Ok(mut cursor) = cursor {
            while let Some(Ok(doc)) = cursor.next().await {
                let cid = doc.get("cid").and_then(|v| v.as_str())
                    .or_else(|| doc.get("_id").and_then(|v| v.as_str()));
                let hash_str = doc.get("iroh_hash").and_then(|v| v.as_str());
                if let (Some(cid), Some(hash_str)) = (cid, hash_str) {
                    if let Ok(hash) = hash_str.parse::<IrohHash>() {
                        let hash_bytes: [u8; 32] = hash.into();
                        map.insert(cid.to_string(), hash_bytes);
                    }
                }
            }
        }
        map
    });
    println!("{} Found {} CID→iroh_hash mappings", ts(), cid_to_hash.len());
    let cid_to_hash = Arc::new(cid_to_hash);

    // Always load existing blob hashes from redb (used for --mappings-only and --skip-existing)
    println!("{} Loading existing blob hashes from redb store...", ts());
    let txn = db.begin_read()?;
    let table = txn.open_table(BLOBS_TABLE)?;
    let existing_hashes: std::collections::HashSet<[u8; 32]> = {
        let mut hs = std::collections::HashSet::new();
        for entry in table.iter()? {
            let (hash, _) = entry?;
            hs.insert(hash.value().into());
        }
        hs
    };
    println!("{} Found {} existing blobs in Iroh store", ts(), existing_hashes.len());
    let existing_hashes = Arc::new(existing_hashes);

    // Filter out CIDs whose blobs already exist in the Iroh redb store
    let cids: Vec<String> = if skip_existing {
        // Filter CIDs — keep only those whose blobs are NOT in the Iroh store
        let mut filtered = Vec::new();
        let mut skipped = 0;
        let mut no_mapping = 0;
        for cid in cids {
            if let Some(hash) = cid_to_hash.get(&cid) {
                if existing_hashes.contains(hash) {
                    skipped += 1;
                } else {
                    filtered.push(cid);
                }
            } else {
                no_mapping += 1;
                filtered.push(cid);
            }
        }
        println!("{} {} CIDs need migration (skipped {} existing, {} no mapping)", ts(), filtered.len(), skipped, no_mapping);
        filtered
    } else {
        cids
    };
    let total = cids.len();
    if total == 0 {
        println!("{} Nothing to migrate — all blobs already in Iroh store.", ts());
        return Ok(());
    }

    let mode = if direct_read { "direct disk read" } else { "Kubo API" };
    let mode = if mappings_only { format!("{} + mappings-only", mode) } else { mode.to_string() };
    println!("{} Starting migration with {} workers, batch_size={}, mode={}", ts(), workers, BATCH_SIZE, mode);

    use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
    use crossbeam_channel::bounded;

    let migrated = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Channel: workers send processed blobs to DB committer
    let (proc_tx, proc_rx) = bounded::<(String, [u8; 32], String, u64)>(workers * 4);

    // DB committer thread: batches redb writes + MongoDB mapping writes
    let db_thread = {
        let db = db.clone();
        let migrated = migrated.clone();
        let failed = failed.clone();
        let total_bytes = total_bytes.clone();
        let total_count = total;
        let start = start.clone();
        let collection = collection.clone();
        let rt = rt.clone();

        std::thread::spawn(move || {
            let mut batch: Vec<(IrohHash, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);
            let mut batch_bytes: u64 = 0;
            let mut mongo_batch: Vec<mongodb::bson::Document> = Vec::with_capacity(BATCH_SIZE);

            while let Ok((cid, hash_bytes, hash_hex, size)) = proc_rx.recv() {
                let iroh_hash = IrohHash::from_bytes(hash_bytes);
                let entry = EntryState::Complete {
                    data_location: DataLocation::Owned(size),
                    outboard_location: OutboardLocation::Owned,
                };
                let entry_bytes = postcard::to_allocvec(&entry).unwrap();

                batch.push((iroh_hash, entry_bytes));
                batch_bytes += size;

                // Build MongoDB mapping document
                let now = mongodb::bson::DateTime::now();
                mongo_batch.push(mongodb::bson::doc! {
                    "_id": &cid,
                    "iroh_hash": &hash_hex,
                    "size": size as i64,
                    "migrated_at": now,
                    "pinned": true,
                });

                if batch.len() >= BATCH_SIZE {
                    if let Err(e) = batch_commit_db(&db, &batch) {
                        eprintln!("{} DB batch commit failed: {}", ts(), e);
                        failed.fetch_add(batch.len(), Ordering::Relaxed);
                    } else {
                        // Write MongoDB mappings
                        let coll = collection.clone();
                        let mb = std::mem::take(&mut mongo_batch);
                        rt.block_on(async move {
                            match coll.insert_many(mb).ordered(false).await {
                                Ok(_) => {},
                                Err(_) => {
                                    // Duplicate key errors are OK (already mapped)
                                }
                            }
                        });
                        let m = migrated.fetch_add(batch.len(), Ordering::Relaxed) + batch.len();
                        total_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
                        if m % 100 < BATCH_SIZE || m == batch.len() {
                            let elapsed = start.elapsed().as_secs_f64();
                            let avg = m as f64 / elapsed;
                            let tb = total_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0;
                            let remaining = total_count - m;
                            let eta = if avg > 0.0 { remaining as f64 / avg } else { 0.0 };
                            let eta_min = eta / 60.0;
                            let pct = m as f64 / total_count as f64 * 100.0;
                            println!("{} [{}/{}] {:.1}% Migrated {} ({:.1} MB, avg {:.1} CID/s, ETA {:.0} min)", ts(), m, total_count, pct, m, tb, avg, eta_min);
                        }
                    }
                    batch.clear();
                    batch_bytes = 0;
                }
            }

            // Final batch
            if !batch.is_empty() {
                if let Err(e) = batch_commit_db(&db, &batch) {
                    eprintln!("{} Final DB commit failed: {}", ts(), e);
                    failed.fetch_add(batch.len(), Ordering::Relaxed);
                } else {
                    let coll = collection.clone();
                    let mb = std::mem::take(&mut mongo_batch);
                    rt.block_on(async move {
                        match coll.insert_many(mb).ordered(false).await {
                            Ok(_) => {},
                            Err(_) => {},
                        }
                    });
                    let m = migrated.fetch_add(batch.len(), Ordering::Relaxed) + batch.len();
                    total_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
                    println!("{} Final batch: {} blobs committed (total {})", ts(), batch.len(), m);
                }
            }
        })
    };

    // Producer
    let (tx, rx) = bounded::<String>(workers * 2);
    let producer = std::thread::spawn(move || {
        for cid in cids {
            let _ = tx.send(cid);
        }
    });

    // Workers: fetch content, compute outboard, write files, send to committer
    let mut handles = vec![];
    for _ in 0..workers {
        let rx = rx.clone();
        let data_dir = data_dir.to_path_buf();
        let kubo_api = kubo_api.clone();
        let blocks_dir = blocks_dir.to_path_buf();
        let proc_tx = proc_tx.clone();
        let failed = failed.clone();
        let direct_read = direct_read;
        let cid_to_hash = cid_to_hash.clone();
        let blobs_dir = data_dir.to_path_buf();
        let existing_hashes = existing_hashes.clone();
        let mappings_only = mappings_only;

        let h = std::thread::spawn(move || {
            let reader = BlockReader {
                blocks_dir: blocks_dir.clone(),
                blobs_dir,
                cid_to_iroh: cid_to_hash,
            };
            while let Ok(cid) = rx.recv() {
                let data_result = if direct_read {
                    reader.read_cid_content(&cid)
                } else {
                    kubo_cat(&kubo_api, &cid)
                };

                match data_result {
                    Ok(data) => {
                        let size = data.len() as u64;
                        match compute_hash(&data) {
                            Ok((hash_bytes, hash_hex, outboard)) => {
                                // Skip blob write if --mappings-only and hash already in redb
                                let already_exists = existing_hashes.contains(&hash_bytes);
                                if !mappings_only || !already_exists {
                                    if let Err(e) = write_blob(&data_dir, &hash_hex, &data, &outboard) {
                                        failed.fetch_add(1, Ordering::Relaxed);
                                        eprintln!("{} Failed to write blob {}: {}", ts(), cid, e);
                                        continue;
                                    }
                                }
                                let _ = proc_tx.send((cid, hash_bytes, hash_hex, size));
                            }
                            Err(e) => {
                                failed.fetch_add(1, Ordering::Relaxed);
                                eprintln!("{} Failed to process {}: {}", ts(), cid, e);
                            }
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!("{} Failed to read {}: {}", ts(), cid, e);
                    }
                }
            }
        });
        handles.push(h);
    }

    drop(rx);
    drop(proc_tx);
    producer.join().ok();
    for h in handles {
        h.join().ok();
    }
    db_thread.join().ok();

    let m = migrated.load(Ordering::Relaxed);
    let f = failed.load(Ordering::Relaxed);
    let tb = total_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0 / 1024.0;
    let elapsed = start.elapsed().as_secs_f64();
    println!("{} === Migration complete ===", ts());
    println!("{}   Migrated: {}", ts(), m);
    println!("{}   Failed: {}", ts(), f);
    println!("{}   Total data: {:.2} GB", ts(), tb);
    println!("{}   Elapsed: {:.1} min", ts(), elapsed / 60.0);
    println!("{}   Rate: {:.1} CID/s", ts(), m as f64 / elapsed);

    Ok(())
}
