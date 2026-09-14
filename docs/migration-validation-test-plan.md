# Post-Migration Validation Test Plan — *.kalou.net Services

**Objective**: Validate all Traefik-defined `*.kalou.net` services after the
IPFS → Iroh migration cutover. The migration replaced Kubo/IPFS Cluster with
`iroh-shim` as the compatibility layer. CID-to-Iroh mappings live in MongoDB
(`hub.cid_iroh_mappings`), and Iroh blobs are stored at `/data/iroh/blobs`.

**Scope**: 79 unique `*.kalou.net` hostnames defined in `compose.yml` Traefik
labels and `traefik/dynamic/services/http/*.yml` file-based routers.

**Execution**: All `curl` commands run from the Mac (external Traefik entrypoint
on port 80/443 via `telour` VPS, or directly via LAN at `192.168.1.244:80`).
Use `https://` for public, `http://` for LAN-internal. Replace `<CID>` with a
known-migrated CID (see sample CIDs below).

**Sample migrated CIDs** (pick from MongoDB `cid_iroh_mappings` with
`iroh_hash` populated and `size > 1MB`):

```bash
# Get 5 sample CIDs with known Iroh hashes
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.find({iroh_hash:{\$exists:true}, size:{\$gt:1048576}}, {cid:1, iroh_hash:1, size:1}).limit(5).toArray()
'"
```

---

## Phase 1 — iroh-shim Core Endpoints (Critical Path)

These are the compatibility layer endpoints that all IPFS-dependent services
rely on. If any fail, the entire migration is broken.

### 1.1 Gateway — `ipfs.kalou.net`

| # | Test | Command | Expected |
|---|------|---------|----------|
| 1 | GET known CID | `curl -sS -o /dev/null -w '%{http_code} %{time_total}s' http://ipfs.kalou.net/ipfs/<CID>` | `200` + <2s |
| 2 | GET unknown CID | `curl -sS -o /dev/null -w '%{http_code}' http://ipfs.kalou.net/ipfs/QmUnknownDoesNotExist000` | `404` |
| 3 | HEAD known CID | `curl -sS -I http://ipfs.kalou.net/ipfs/<CID>` | `200` + `Content-Length` present |
| 4 | Range request | `curl -sS -r 0-1023 -o /dev/null -w '%{http_code} %{size_download}' http://ipfs.kalou.net/ipfs/<CID>` | `206` + `1024` |
| 5 | Content-Type header | `curl -sS -I http://ipfs.kalou.net/ipfs/<CID> \| grep -i content-type` | Valid MIME type |

### 1.2 API — `ipfs-api.kalou.net`

| # | Test | Command | Expected |
|---|------|---------|----------|
| 6 | `/api/v0/version` | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/version` | `{"Version":"0.0.1-iroh-shim",...}` |
| 7 | `/api/v0/cat` known CID | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/cat?arg=<CID> -o /dev/null -w '%{http_code} %{time_total}s'` | `200` + <2s |
| 8 | `/api/v0/cat` unknown CID | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/cat?arg=QmUnknown -o /dev/null -w '%{http_code}'` | `404` |
| 9 | `/api/v0/pin/ls` | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/pin/ls` | `200` + JSON with `Keys` |
| 10 | `/api/v0/block/stat` | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/block/stat?arg=<CID>` | `200` + JSON with `Size` |
| 11 | `/api/v0/add` (if RW mount) | `echo "test" \| curl -sS -X POST -F file=@- http://ipfs-api.kalou.net/api/v0/add` | `200` + JSON with `Hash` |
| 12 | `/api/v0/repo/stat` | `curl -sS -X POST http://ipfs-api.kalou.net/api/v0/repo/stat` | `200` + JSON with `RepoSize` |

### 1.3 Gateway aliases — `ipns.kalou.net`, `cid.kalou.net`

| # | Test | Command | Expected |
|---|------|---------|----------|
| 13 | `ipns.kalou.net` gateway | `curl -sS -o /dev/null -w '%{http_code}' http://ipns.kalou.net/ipfs/<CID>` | `200` |
| 14 | `cid.kalou.net` gateway | `curl -sS -o /dev/null -w '%{http_code}' http://cid.kalou.net/ipfs/<CID>` | `200` |

---

## Phase 2 — IPFS-Dependent Web Apps (Serve `/ipfs/` Content)

These apps have Traefik routers with `PathPrefix(\`/ipfs/\`)` routing to
`iroh-shim:5001`. They embed IPFS content (videos, images, thumbnails).

### 2.1 Video apps (HLS/streaming)

| # | Host | Test | Expected |
|---|------|------|----------|
| 15 | `porntoc.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://porntoc.kalou.net/` | `200` |
| 16 | `porntoc.kalou.net` | `curl -sS -o /dev/null -w '%{http_code} %{time_total}s' https://porntoc.kalou.net/ipfs/<CID>` | `200` + <2s |
| 17 | `tvid.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://tvid.kalou.net/` | `200` |
| 18 | `tvid.kalou.net` | `curl -sS -o /dev/null -w '%{http_code} %{time_total}s' https://tvid.kalou.net/ipfs/<CID>` | `200` + <2s |
| 19 | `vagina.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://vagina.kalou.net/` | `200` |
| 20 | `vagina.kalou.net` | `curl -sS -o /dev/null -w '%{http_code} %{time_total}s' https://vagina.kalou.net/ipfs/<CID>` | `200` + <2s |
| 21 | `vitube.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://vitube.kalou.net/` | `200` |
| 22 | `vitube.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://vitube.kalou.net/ipfs/<CID>` | `200` |
| 23 | `cinegrid.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://cinegrid.kalou.net/` | `200` |
| 24 | `cinegrid.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://cinegrid.kalou.net/ipfs/<CID>` | `200` |
| 25 | `pornmap.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://pornmap.kalou.net/` | `200` |
| 26 | `pornmap.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://pornmap.kalou.net/ipfs/<CID>` | `200` |

### 2.2 Content browsing apps

| # | Host | Test | Expected |
|---|------|------|----------|
| 27 | `xplore.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://xplore.kalou.net/` | `200` |
| 28 | `xplore.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://xplore.kalou.net/ipfs/<CID>` | `200` |
| 29 | `music.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://music.kalou.net/` | `200` |
| 30 | `music.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://music.kalou.net/ipfs/<CID>` | `200` |
| 31 | `authors.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://authors.kalou.net/` | `200` |
| 32 | `authors.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://authors.kalou.net/ipfs/<CID>` | `200` |
| 33 | `s.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://s.kalou.net/` | `200` |
| 34 | `s.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://s.kalou.net/ipfs/<CID>` | `200` |
| 35 | `list.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://list.kalou.net/` | `200` (alias of hub) |

---

## Phase 3 — IPFS API Consumers (Backend Services)

These services call `iroh-shim:5001` internally via `IPFS_API`,
`IPFS_API_URL`, `IPFS_GATEWAY_URL`, `KUBO_API`, etc.

| # | Host | Env var repointed | Test | Expected |
|---|------|-------------------|------|----------|
| 36 | `hub.kalou.net` | `IPFS_API`, `IPFS_GATEWAY` | `curl -sS -o /dev/null -w '%{http_code}' https://hub.kalou.net/` | `200` |
| 37 | `hub.kalou.net` | — | `curl -sS https://hub.kalou.net/api/ipfs-pinner` | `200` or valid JSON |
| 38 | `ipfs-pinner.kalou.net` | `IPFS_PIN_API` | `curl -sS -o /dev/null -w '%{http_code}' https://ipfs-pinner.kalou.net/` | `200` |
| 39 | `ipfs-indexer.kalou.net` | `IPFS_API_URL` | `curl -sS -o /dev/null -w '%{http_code}' https://ipfs-indexer.kalou.net/` | `200` (or 503 if scaled down) |
| 40 | `video-indexer.kalou.net` | `IPFS_API_URL` | `curl -sS -o /dev/null -w '%{http_code}' https://video-indexer.kalou.net/` | `200` |
| 41 | `video-processor.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://video-processor.kalou.net/` | `200` |
| 42 | `image-indexer.kalou.net` | `IPFS_API_URL` | `curl -sS -o /dev/null -w '%{http_code}' https://image-indexer.kalou.net/` | `200` |
| 43 | `image-processor.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://image-processor.kalou.net/` | `200` |
| 44 | `content-scanner.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://content-scanner.kalou.net/` | `200` |
| 45 | `content-ingestion.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://content-ingestion.kalou.net/` | `200` |
| 46 | `snr-analyzer.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://snr-analyzer.kalou.net/` | `200` |
| 47 | `yt.kalou.net` | `IPFS_GATEWAY` | `curl -sS -o /dev/null -w '%{http_code}' https://yt.kalou.net/` | `200` |
| 48 | `yt-indexer.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://yt-indexer.kalou.net/` | `200` |
| 49 | `backup-service.kalou.net` | `IPFS_API` | `curl -sS -o /dev/null -w '%{http_code}' https://backup-service.kalou.net/` | `200` |

---

## Phase 4 — Non-IPFS Services (Smoke Test)

These services don't depend on IPFS but should still be reachable through
Traefik. Quick HTTP status check only.

### 4.1 Web apps

| # | Host | Test | Expected |
|---|------|------|----------|
| 50 | `tags.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://tags.kalou.net/` | `200` |
| 51 | `stats.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://stats.kalou.net/` | `200` |
| 52 | `shops.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://shops.kalou.net/` | `200` |
| 53 | `prices.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://prices.kalou.net/` | `200` |
| 54 | `pubmed.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://pubmed.kalou.net/` | `200` |
| 55 | `pubgrid.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://pubgrid.kalou.net/` | `200` |
| 56 | `voice.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://voice.kalou.net/` | `200` |
| 57 | `v.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://v.kalou.net/` | `200` |
| 58 | `md.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://md.kalou.net/` | `200` |
| 59 | `chat.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://chat.kalou.net/` | `200` |
| 60 | `desk.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://desk.kalou.net/` | `200` |
| 61 | `events.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://events.kalou.net/` | `200` |
| 62 | `files.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://files.kalou.net/` | `200` |
| 63 | `gallery.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://gallery.kalou.net/` | `200` |
| 64 | `lens.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://lens.kalou.net/` | `200` |
| 65 | `portfolio.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://portfolio.kalou.net/` | `200` |
| 66 | `b-corp.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://b-corp.kalou.net/` | `200` |
| 67 | `libre.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://libre.kalou.net/` | `200` |
| 68 | `sie.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://sie.kalou.net/` | `200` |
| 69 | `vm.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://vm.kalou.net/` | `200` |
| 70 | `bt.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://bt.kalou.net/` | `200` |
| 71 | `dht.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://dht.kalou.net/` | `200` |
| 72 | `api.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://api.kalou.net/` | `200` |
| 73 | `cid-router.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://cid-router.kalou.net/` | `200` |

### 4.2 Search / indexing tools

| # | Host | Test | Expected |
|---|------|------|----------|
| 74 | `x.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://x.kalou.net/` | `200` |
| 75 | `search.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://search.kalou.net/` | `200` |
| 76 | `searxng.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://searxng.kalou.net/` | `200` |
| 77 | `xng.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://xng.kalou.net/` | `200` |
| 78 | `metager.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://metager.kalou.net/` | `200` |
| 79 | `flare.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://flare.kalou.net/` | `200` |

### 4.3 Utility / admin services

| # | Host | Test | Expected |
|---|------|------|----------|
| 80 | `music-index.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://music-index.kalou.net/` | `200` |
| 81 | `music-indexer.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://music-indexer.kalou.net/` | `200` |
| 82 | `ai-tag-discovery.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://ai-tag-discovery.kalou.net/health` | `200` |
| 83 | `tcm.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://tcm.kalou.net/` | `200` |
| 84 | `receipt-import.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://receipt-import.kalou.net/` | `200` |
| 85 | `jackett-general.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://jackett-general.kalou.net/` | `200` |
| 86 | `jackett-movies.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://jackett-movies.kalou.net/` | `200` |
| 87 | `jackett-tv.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://jackett-tv.kalou.net/` | `200` |
| 88 | `jackett-books.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://jackett-books.kalou.net/` | `200` |
| 89 | `gnucash.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://gnucash.kalou.net/` | `200` |
| 90 | `ocr.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://ocr.kalou.net/` | `200` |
| 91 | `exif.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://exif.kalou.net/` | `200` |
| 92 | `garbage.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://garbage.kalou.net/` | `200` |
| 93 | `dict-sucker.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://dict-sucker.kalou.net/` | `200` |
| 94 | `monitor.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://monitor.kalou.net/` | `200` |
| 95 | `video-downloader.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://video-downloader.kalou.net/` | `200` |
| 96 | `document-processor.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://document-processor.kalou.net/` | `200` |
| 97 | `grafana.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://grafana.kalou.net/` | `200` or `302` (login redirect) |
| 98 | `jaeger.kalou.net` | `curl -sS -o /dev/null -w '%{http_code}' https://jaeger.kalou.net/` | `200` |

---

## Phase 5 — Data Integrity Verification

### 5.1 MongoDB mapping completeness

```bash
# Total mappings
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  print(\"total: \" + db.cid_iroh_mappings.countDocuments({}));
  print(\"with iroh_hash: \" + db.cid_iroh_mappings.countDocuments({iroh_hash:{\$exists:true}}));
  print(\"with cid: \" + db.cid_iroh_mappings.countDocuments({cid:{\$exists:true}}));
  print(\"pinned: \" + db.cid_iroh_mappings.countDocuments({pinned:true}));
'"
```

Expected: `total` ≈ 966K, `with iroh_hash` = `total` (100%).

### 5.2 Iroh blob store integrity

```bash
# Blob count on disk
ssh pvf "find /data/iroh/blobs/data/ -name '*.data' | wc -l"
# redb record count
ssh pvf "ls -la /data/iroh/blobs/blobs.db"
# Disk usage
ssh pvf "df -h /data/iroh"
```

Expected: ~807K blob files, ~372 GB used, <15% of 3.7 TB.

### 5.3 Sample CID content verification

```bash
# Pick 10 random migrated CIDs and verify they return content
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.aggregate([{\$match:{iroh_hash:{\$exists:true},size:{\$gt:1048576}}},{\$sample:{size:10}}]).forEach(function(d){
    print(d.cid + \" \" + d.iroh_hash + \" \" + d.size);
  })
'" | while read cid hash size; do
  actual_size=$(curl -sS -o /dev/null -w '%{size_download}' http://ipfs.kalou.net/ipfs/$cid)
  echo "$cid: expected=$size actual=$actual_size $([ "$size" -eq "$actual_size" ] && echo OK || echo MISMATCH)"
done
```

Expected: All 10 samples show `OK` (expected size matches actual).

### 5.4 Video playback spot-check

Manually open in browser:
- `https://porntoc.kalou.net/` — click a video, verify it plays
- `https://tvid.kalou.net/` — click a video, verify it plays
- `https://vagina.kalou.net/` — click a video, verify it plays
- `https://cinegrid.kalou.net/` — click a film, verify it plays

Check: video loads within 3s, seeking works, no truncation.

---

## Phase 6 — Service Health on pvf

### 6.1 Docker Swarm service status

```bash
ssh pvf "docker service ls --format '{{.Name}}\t{{.Replicas}}\t{{.Image}}' | grep -E 'iroh|ipfs|rainbow|hub|video|image|content|snr|yt|backup'"
```

Expected:
- `iroh-shim` — `1/1` — `o-iroh-shim:latest`
- `ipfs` — `0/0` (scaled down)
- `rainbow-gateway` — `0/0` (scaled down)
- `rainbow-front` — `0/0` (scaled down)
- All dependent services — `1/1` (running)

### 6.2 iroh-shim logs (error check)

```bash
ssh pvf "docker service logs iroh-shim --since 10m 2>&1 | grep -iE 'error|traceback|exception' | tail -20"
```

Expected: No errors, or only expected 404s for unknown CIDs.

### 6.3 Traefik router health

```bash
# Check all iroh-related routers are registered
ssh pvf "docker exec \$(docker ps -q -f name=traefik) wget -qO- http://localhost:8080/api/http/routers 2>/dev/null" | \
  jq '.[] | select(.rule | test("ipfs|iroh|cid")) | {name: .name, rule: .rule, status: .status, server: .service}'
```

Expected: `iroh-api`, `iroh-gw` routers with `status: "enabled"` pointing to
`iroh-shim:5001`. No `ipfs-api@file` or `rainbow` routers pointing to dead
`ipfs:5001` or `rainbow-front:80`.

---

## Phase 7 — Regression & Edge Cases

### 7.1 Bookmark/thumbnail CIDs

```bash
# Video bookmarks use thumbnail CIDs — verify they resolve
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.video_bookmarks.find({thumbnail_cid:{\$exists:true}}).limit(5).forEach(function(b){
    print(b.thumbnail_cid);
  })
'" | while read cid; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' http://ipfs.kalou.net/ipfs/$cid)
  echo "$cid: $code"
done
```

Expected: All `200`.

### 7.2 Large file streaming (range requests)

```bash
# Pick a large CID (>50MB) and test range request
LARGE_CID=$(ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.findOne({size:{\$gt:52428800}}).cid
'")
# First 1MB
curl -sS -r 0-1048575 -o /dev/null -w 'first_1MB: %{http_code} %{size_download} %{time_total}s\n' http://ipfs.kalou.net/ipfs/$LARGE_CID
# Last 1MB
SIZE=$(ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.findOne({cid:\"$LARGE_CID\"}).size
'")
LAST_OFFSET=$((SIZE - 1048576))
curl -sS -r ${LAST_OFFSET}-$((SIZE - 1)) -o /dev/null -w 'last_1MB: %{http_code} %{size_download} %{time_total}s\n' http://ipfs.kalou.net/ipfs/$LARGE_CID
# Middle 1MB
MID_OFFSET=$((SIZE / 2))
curl -sS -r ${MID_OFFSET}-$((MID_OFFSET + 1048575)) -o /dev/null -w 'mid_1MB: %{http_code} %{size_download} %{time_total}s\n' http://ipfs.kalou.net/ipfs/$LARGE_CID
```

Expected: All `206` with `1048576` bytes, <1s each.

### 7.3 Concurrent requests

```bash
# 20 concurrent requests for different CIDs
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.aggregate([{\$match:{iroh_hash:{\$exists:true}}},{\$sample:{size:20}}]).forEach(function(d){
    print(d.cid);
  })
'" | xargs -P20 -I{} curl -sS -o /dev/null -w '{} %{http_code} %{time_total}s\n' http://ipfs.kalou.net/ipfs/{}
```

Expected: All `200`, no timeouts, <3s each.

### 7.4 `/api/v0/add` round-trip

```bash
# Upload a test file and verify it's retrievable
echo "migration-test-$(date +%s)" > /tmp/test_upload.txt
RESULT=$(curl -sS -X POST -F file=@/tmp/test_upload.txt http://ipfs-api.kalou.net/api/v0/add)
echo "$RESULT"
CID=$(echo "$RESULT" | jq -r '.Hash')
# Verify retrieval
curl -sS -X POST "http://ipfs-api.kalou.net/api/v0/cat?arg=$CID"
# Verify in MongoDB mapping
ssh pvf "docker exec mongo mongosh hub --quiet --eval '
  db.cid_iroh_mappings.findOne({cid:\"$CID\"})
'"
```

Expected: Upload returns a CID, retrieval returns the same content, MongoDB
mapping exists with `iroh_hash`.

---

## Pass/Fail Criteria

| Phase | Pass criteria |
|-------|---------------|
| 1 — iroh-shim core | All 14 tests pass (200/404 as expected) |
| 2 — IPFS web apps | All app homepages return 200; all `/ipfs/<CID>` return 200 |
| 3 — API consumers | All services return 200 (or 503 if intentionally scaled down) |
| 4 — Non-IPFS services | All return 200 (or expected redirect) |
| 5 — Data integrity | 100% mappings have `iroh_hash`; 10/10 sample CIDs size-match |
| 6 — Service health | iroh-shim 1/1; old services 0/0; no error logs |
| 7 — Regression | Range requests, concurrent, add round-trip all pass |

**Rollback condition**: If Phase 1 or Phase 5 fails, the migration is broken.
Scale `ipfs` back up (`docker service scale ipfs=1`) and revert Traefik dynamic
configs to point to `ipfs:5001`/`ipfs:5002` until the issue is resolved.

---

## Automated Bulk Test Script

```bash
#!/bin/bash
# Run all Phase 1-4 HTTP tests in parallel, output results table
# Usage: bash migration-validation-test-plan.md.run <SAMPLE_CID>

CID="${1:-QmUnknown}"
BASE="http://ipfs.kalou.net"
API="http://ipfs-api.kalou.net"

hosts=(
  ipfs.kalou.net ipns.kalou.net cid.kalou.net ipfs-api.kalou.net
  porntoc.kalou.net tvid.kalou.net vagina.kalou.net vitube.kalou.net
  cinegrid.kalou.net pornmap.kalou.net xplore.kalou.net music.kalou.net
  authors.kalou.net s.kalou.net list.kalou.net hub.kalou.net
  ipfs-pinner.kalou.net ipfs-indexer.kalou.net video-indexer.kalou.net
  video-processor.kalou.net image-indexer.kalou.net image-processor.kalou.net
  content-scanner.kalou.net content-ingestion.kalou.net snr-analyzer.kalou.net
  yt.kalou.net yt-indexer.kalou.net backup-service.kalou.net
  tags.kalou.net stats.kalou.net shops.kalou.net prices.kalou.net
  pubmed.kalou.net pubgrid.kalou.net voice.kalou.net v.kalou.net
  md.kalou.net chat.kalou.net desk.kalou.net events.kalou.net
  files.kalou.net gallery.kalou.net lens.kalou.net portfolio.kalou.net
  b-corp.kalou.net libre.kalou.net sie.kalou.net vm.kalou.net
  bt.kalou.net dht.kalou.net api.kalou.net cid-router.kalou.net
  x.kalou.net search.kalou.net searxng.kalou.net xng.kalou.net
  metager.kalou.net flare.kalou.net music-index.kalou.net
  music-indexer.kalou.net ai-tag-discovery.kalou.net tcm.kalou.net
  receipt-import.kalou.net jackett-general.kalou.net jackett-movies.kalou.net
  jackett-tv.kalou.net jackett-books.kalou.net gnucash.kalou.net
  ocr.kalou.net exif.kalou.net garbage.kalou.net dict-sucker.kalou.net
  monitor.kalou.net video-downloader.kalou.net document-processor.kalou.net
  grafana.kalou.net jaeger.kalou.net
)

printf "%-35s %s\n" "HOST" "STATUS  TIME"
printf "%-35s %s\n" "----" "------  ----"
for h in "${hosts[@]}"; do
  result=$(curl -sS -o /dev/null -w '%{http_code} %{time_total}s' --max-time 10 "https://$h/" 2>&1)
  printf "%-35s %s\n" "$h" "$result"
done

echo ""
echo "=== IPFS gateway tests ==="
for h in ipfs.kalou.net ipns.kalou.net cid.kalou.net porntoc.kalou.net tvid.kalou.net \
         vagina.kalou.net vitube.kalou.net cinegrid.kalou.net pornmap.kalou.net \
         xplore.kalou.net music.kalou.net authors.kalou.net s.kalou.net; do
  result=$(curl -sS -o /dev/null -w '%{http_code} %{time_total}s' --max-time 10 "https://$h/ipfs/$CID" 2>&1)
  printf "%-35s /ipfs/%s  %s\n" "$h" "$CID" "$result"
done

echo ""
echo "=== API tests ==="
curl -sS -X POST "$API/api/v0/version" | head -c 200
echo ""
curl -sS -o /dev/null -w 'cat: %{http_code} %{time_total}s\n' -X POST "$API/api/v0/cat?arg=$CID"
curl -sS -o /dev/null -w 'pin/ls: %{http_code}\n' -X POST "$API/api/v0/pin/ls"
curl -sS -o /dev/null -w 'repo/stat: %{http_code}\n' -X POST "$API/api/v0/repo/stat"
```
