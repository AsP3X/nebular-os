# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Nebular OS is a single-binary Rust/Axum object storage service with embedded SQLite. No external services are required. See `README.md` for the full API and configuration reference.

### Prerequisites (handled by update script)

- **Rust stable >= 1.85** (`edition = "2024"` in `Cargo.toml`). The update script runs `rustup update stable`.
- **`libssl-dev` + `pkg-config`** on Ubuntu — needed by the `reqwest` dev-dependency (used only in `tests/integration.rs`). The update script installs them.

### Running the server

```bash
cp .env.example .env   # only once; set NOS_JWT_SECRET and NOS_SIGNING_SECRET (each >= 32 chars)
cargo run              # listens on NOS_BIND_ADDR (default 0.0.0.0:9000)
```

The `.env` file is gitignored. If it already exists, `cargo run` will load it automatically via `dotenvy`.

### Key commands

| Task | Command |
|------|---------|
| Build | `cargo build` |
| Test | `cargo test` |
| Lint | `cargo clippy` |
| Run (dev) | `cargo run` |

### Gotchas

- **JWT for API calls:** Generate an HS256 JWT with claims `{sub, email, role, iat, exp}` signed with the value of `NOS_JWT_SECRET` from `.env`. Pass it as `Authorization: Bearer <token>`.
- **Integration tests are self-contained:** `cargo test` creates temp dirs and does not need a running server.
- **Clippy has pre-existing warnings** on `master` (collapsible-if, manual-strip, etc.). These are in existing code and are not regressions. Running `cargo clippy` (without `-D warnings`) will show them but succeed.
- **No hot-reload:** After code changes, stop and re-run `cargo run`. There is no watch mode configured by default.
