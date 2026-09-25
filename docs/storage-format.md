# Nebular OS storage format

This document describes what Nebular OS keeps under `NOS_DATA_DIR`: the directory layout, the blob formats and
the files that support them. Every multi-byte integer is **little-endian**. Hashes are **xxh3-64** unless noted.

The formats a release writes change over time; every format listed here stays readable. `tests/fixtures/`
holds data directories written by released builds, and `tests/upgrade_fixtures.rs` checks on every run that the
current code reads, scrubs, recompresses and migrates all of it.

## Directory layout

```
NOS_DATA_DIR/
├── {bucket}/{shard}/{filename}   object blobs (current layout)
├── {bucket}/{shard}/{key path}   object blobs (legacy nested layout, read and delete only)
├── .blocks/{hh}/{hash}           deduplicated blocks (NOSK or raw)
├── .dict/{id}.zdict              trained zstd dictionaries
├── .multipart/{upload_id}/{nnnnn} uploaded multipart parts (zero-padded part number)
└── .tmp/                         scratch files and the overwrite journal
```

- **`{shard}`** is `xxh3_64(key) & 0xff` as two lowercase hex digits.
- **`{filename}`** is the object key as one path segment, so keys sharing a prefix never collide with a
  directory. `/` becomes `%2F` and `%` becomes `%25`; nothing else is escaped.
  - On case-insensitive or Windows filesystems (detected when the engine starts), the filename is
    **portable** instead: only lowercase ASCII letters, digits and ``-_.()[]{}+,;=@!#$&'~^` `` plus space are kept.
    Every other byte is written as `%XX` (uppercase hex): uppercase letters, non-ASCII bytes, control
    characters, Windows-invalid characters, a trailing `.` or space, and the first letter of a reserved
    device name such as `con` or `com1`. Two keys that differ only in case therefore get different files.
  - Readers try the current filename, then the plain filename (on portable filesystems), then the legacy
    nested path.
- New writes reject keys whose filename would exceed 255 bytes. Bucket names never start with `.`, so they
  can't collide with the system directories.

## Detecting a blob's format

The first bytes say which format a blob is:

| Magic  | Format | Written by |
|--------|--------|------------|
| `NOSI` | Indexed blocks, v1 | Current writes, recompression and migration |
| `NOSB` | Indexed blocks, v0 | Unreleased builds before NOSI |
| `NOS2` | zstd stream with dictionary id and level | 0.1.2–0.1.4 |
| `NOSZ` | zstd stream | Releases before 0.1.2 |
| `NOSD` | Dedup manifest | 0.1.2–0.1.4 with `NOS_DEDUP_ENABLED` |
| other  | Raw object bytes | Incompressible objects and objects below `NOS_COMPRESS_MIN_SIZE` |

Every container keeps the object's logical size at bytes 4–12. When those bytes disagree with the size in the
metadata and the file is exactly the object's size, the blob is **raw**: the object's own content just happens
to start with a magic. Current writes wrap such uploads in NOSI anyway.

## NOSI — indexed blocks, v1

The object is split into blocks of `block_size` logical bytes (the last one may be shorter). Each block is
compressed, stored or deduplicated on its own, and an index at the front locates every block without reading
the others, so ranges and seeks decode only the blocks they need.

**Fixed header** (24 bytes, or 25 when flag bit 15 is set):

| Offset | Size | Field |
|-------:|-----:|-------|
| 0  | 4 | Magic `NOSI` |
| 4  | 8 | Logical size (bytes) |
| 12 | 4 | Block size (logical bytes per block) |
| 16 | 4 | Block count |
| 20 | 2 | Dictionary id used for the blocks (`0` = none, or the original `0.zdict`) |
| 22 | 2 | Flags: bit 0 = some blocks are dedup references; bit 15 = byte 24 holds the zstd level |
| 24 | 1 | zstd level the blocks were written with (present when flag bit 15 is set) |

**Index**: block count × 16 bytes, directly after the fixed header.

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 8 | Offset of the block's header, counted from the end of the index |
| 8 | 8 | Logical end of the block (exclusive); the last one equals the logical size |

**Blocks** follow the index. Each one has a 16-byte header and a payload:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 1 | Type: `0` zstd frame, `1` stored raw, `2` dedup reference |
| 1 | 3 | Reserved (zero) |
| 4 | 4 | Payload length |
| 8 | 8 | xxh3 of the block's logical bytes; for a dedup reference, the block's content hash |

The payloads by type:

- **Type 0** is one zstd frame.
  - A frame compressed with a dictionary carries that dictionary's id in its frame header, and readers look the
    dictionary up by that id (see [Dictionaries](#dictionaries)). The id in the fixed header tells recompression
    which blobs predate the newest dictionary.
  - Decoding stops at the block's logical length plus one byte, so a corrupt frame can't expand without limit.
- **Type 1** holds the logical bytes as they are.
- **Type 2** is 12 bytes: the content hash (8), then the logical length (4) of a block in `.blocks/`.

Every decoded block is checked against its xxh3 before it is returned.

Writers stream blocks to disk behind a reserved header and write the header last, since the block count follows
from the logical size. If the encoded blob wouldn't be smaller than the object, the object is stored raw
instead, unless it must be wrapped (see [Detecting a blob's format](#detecting-a-blobs-format)).

## NOSB — indexed blocks, v0

NOSB has the same structure as NOSI, with a 20-byte fixed header (magic, logical size, block size, block count)
and 8-byte block headers (type, 3 reserved bytes, payload length). NOSB has no dictionary, flags, level or block
checksums, and only uses block types 0 and 1. Recompression rewrites NOSB blobs as NOSI.

## NOS2 and NOSZ — single zstd stream

| Offset | Size | NOS2 | NOSZ |
|-------:|-----:|------|------|
| 0  | 4 | Magic `NOS2` | Magic `NOSZ` |
| 4  | 8 | Logical size | Logical size |
| 12 | 2 | Dictionary id | zstd stream starts here |
| 14 | 1 | zstd level | |
| 15 | 1 | Reserved | |
| 16 | … | zstd stream | |

The whole object is one zstd stream, so a range read decodes from the start up to the range. Migration
(`POST /_nos/maintenance/migrate_blobs`) and recompression rewrite these blobs as NOSI.

## NOSD — dedup manifest

| Offset | Size | Field |
|-------:|-----:|-------|
| 0  | 4 | Magic `NOSD` |
| 4  | 8 | Logical size |
| 12 | 4 | Entry count |
| 16 | 12 × count | Entries: content hash (8), logical length (4) |

The object is the entries' blocks from `.blocks/`, in order. The file must be exactly `16 + 12 × count` bytes, and
the entry lengths must add up to the logical size.

## `.blocks/` — deduplicated blocks

A block with content hash `h` lives at `.blocks/{hh}/{h:016x}`, where `hh` is the first two hex digits. The file
is either the raw logical bytes or a NOSK wrapper:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | Magic `NOSK` |
| 4 | 4 | Logical length |
| 8 | … | zstd frame |

- The block's address is its checksum: readers verify the bytes against it.
- A new block reuses an existing file only when the bytes are identical. If another block already holds the
  same hash, the new block is kept inline in its own blob instead.
- Reference counts live in the `dedup_blocks` table of the system SQLite database. A block whose count drops to
  zero is deleted by housekeeping after an hour, unless it was used again in the meantime.

## Dictionaries

`.dict/{id}.zdict` holds a trained zstd dictionary.

- New writes use the dictionary with the highest id, when `NOS_ZSTD_DICT_ENABLED` is on.
- Ids start at 1. Id 0 is the single dictionary that releases up to 0.1.4 kept as `0.zdict`; it stays valid.
- A dictionary file is never replaced or removed. Training writes the next id, and frames name their dictionary,
  so older blobs keep decoding.
- Every dictionary in `.dict/` is loaded when the engine starts, whatever `NOS_ZSTD_DICT_ENABLED` says.

## `.tmp/` — scratch files and the overwrite journal

- **Scratch files** belong to uploads, staging, maintenance and replication. Housekeeping deletes any that are
  more than an hour old, counting both their modification and inode-change times.
- **Overwrite journal.** Replacing an existing object's blob writes two files before the new blob is renamed
  over the old one:
  - `{id}.prev`, a hard link to (or copy of) the current blob.
  - `{id}.swap`, a JSON record `{"bucket", "key", "new_etag"}`, fsynced.

  Both files are removed once the metadata names the new version. If the process stops in between, the next
  start finds the `.swap`. If the metadata still names another version, the `.prev` blob is renamed back, so
  bytes and metadata agree again.

## Metadata

Object metadata lives in SQLite (`NOS_META_PATH`) or Postgres (`NOS_METADATA_DATABASE_URL`). The blob path is
derived from bucket and key, so metadata never has to be consulted to find a blob.

The system SQLite database always holds these tables:

- `dedup_blocks`: block reference counts.
- `maintenance_state`: the cursors of the recompression and scrub walks.
- `cluster_runtime_config`
- `replication_log` and `replication_versions` (cluster modes): queued and applied replication events, and the
  version of each replicated key. A deleted key keeps a version marked deleted (a tombstone).

Postgres schema versions are recorded in `nos_schema_migrations`. Indexes added later are built with
`CREATE INDEX CONCURRENTLY` after startup.
