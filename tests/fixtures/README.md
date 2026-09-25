# Upgrade fixtures

`v0.1.4/` is storage written by the released v0.1.4 binary (tag `v0.1.4`), which
`tests/upgrade_from_v0_1_4.rs` opens with the current engine on every test run.

- `data/` is the data directory. It holds NOS2 blobs compressed without and with the trained zstd dictionary
  (`.dict/0.zdict`), NOSD dedup manifests with their blocks in `.blocks/`, raw blobs, and a multipart object.
  Every compressed object that 0.1.2–0.1.4 wrote is NOS2; no release wrote NOSZ, NOSB or NOSI.
- `metadata.db` is the SQLite metadata in v0.1.4's schema, including one soft-deleted object.
- `manifest.json` lists every live object with its size and SHA-256, computed from the uploaded bytes and not
  by the server.

To regenerate (only on purpose; see the script header):

```bash
git worktree add /tmp/nos-v0.1.4 v0.1.4
(cd /tmp/nos-v0.1.4 && CARGO_TARGET_DIR=/tmp/nos-v0.1.4-target cargo build --release)
python3 tests/fixtures/generate_v0_1_4.py /tmp/nos-fixture-scratch /tmp/nos-v0.1.4-target/release/nebular-os tests/fixtures/v0.1.4
```
