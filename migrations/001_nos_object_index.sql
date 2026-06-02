CREATE TABLE IF NOT EXISTS nos_objects (
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    blob_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    content_type TEXT,
    etag TEXT,
    custom_meta TEXT,
    storage_class TEXT DEFAULT 'default',
    origin_node TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (bucket, object_key)
);

CREATE INDEX IF NOT EXISTS idx_nos_objects_deleted ON nos_objects (deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_nos_objects_prefix ON nos_objects (bucket, object_key);

CREATE TABLE IF NOT EXISTS nos_multipart_uploads (
    upload_id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    content_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS nos_multipart_parts (
    upload_id TEXT NOT NULL REFERENCES nos_multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    blob_path TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    etag TEXT,
    PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE IF NOT EXISTS nos_cluster_runtime_config (
    id INTEGER PRIMARY KEY,
    json TEXT NOT NULL
);
