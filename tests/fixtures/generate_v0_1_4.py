"""Regenerate tests/fixtures/v0.1.4 with the released v0.1.4 binary (the output is committed; see README.md).

Usage: python3 tests/fixtures/generate_v0_1_4.py <scratch-dir> <v0.1.4 nebular-os binary> tests/fixtures/v0.1.4
Object content is seeded, but timestamps and upload ids make the output differ run to run, so only
regenerate on purpose.
"""
import atexit, base64, collections, hashlib, hmac, json, os, random, shutil, sqlite3, subprocess, sys, time
SP, OLD, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
D = f"{SP}/fixgen"; shutil.rmtree(D, ignore_errors=True); os.makedirs(f"{D}/data"); os.makedirs(f"{D}/meta")
SECRET = b"fixture-jwt-secret-0123456789abcdef-01234567"; PORT = 19094; BASE = f"http://127.0.0.1:{PORT}"
def b64(b): return base64.urlsafe_b64encode(b).rstrip(b"=")
now = int(time.time())
h = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()); p = b64(json.dumps({"sub": "fixture", "email": "f@x.y", "role": "admin", "iat": now, "exp": now + 3600}).encode())
TOKEN = (h + b"." + p + b"." + b64(hmac.new(SECRET, h + b"." + p, hashlib.sha256).digest())).decode()
servers = []; atexit.register(lambda: [s.terminate() for s in servers if s.poll() is None])
ENV = {"PATH": "/usr/bin:/bin", "HOME": os.environ["HOME"], "NOS_JWT_SECRET": SECRET.decode(), "NOS_BIND_ADDR": f"127.0.0.1:{PORT}",
       "NOS_DATA_DIR": f"{D}/data", "NOS_META_PATH": f"{D}/meta/metadata.db", "RUST_LOG": "warn", "NOS_UPLOAD_MAX_IN_FLIGHT_BYTES": "0",
       "NOS_ZSTD_DICT_ENABLED": "true", "NOS_ZSTD_DICT_MAX_BYTES": "2048", "NOS_ZSTD_DICT_TRAIN_BATCH": "16",
       "NOS_ZSTD_LEVEL": "3", "NOS_ZSTD_LEVEL_UPLOAD": "3",
       "NOS_DEDUP_ENABLED": "true", "NOS_DEDUP_MIN_SIZE": "65536", "NOS_DEDUP_BLOCK_SIZE": "16384"}
def start(**extra):
    if subprocess.run(["curl", "-s", "-o", "/dev/null", f"{BASE}/health"]).returncode == 0: raise SystemExit("port busy")
    srv = subprocess.Popen([OLD], env={**ENV, **extra}, stdout=open(f"{D}/server.log", "a"), stderr=subprocess.STDOUT); servers.append(srv)
    for _ in range(100):
        if srv.poll() is not None: raise SystemExit(open(f"{D}/server.log").read()[-800:])
        if subprocess.run(["curl", "-s", "-o", "/dev/null", f"{BASE}/health"]).returncode == 0: return srv
        time.sleep(0.2)
    raise SystemExit("no start")
def stop(srv): srv.terminate(); srv.wait()
def req(method, path, body=None, headers=()):
    args = ["curl", "-sS", "-X", method, BASE + path, "-H", f"Authorization: Bearer {TOKEN}", "-o", f"{D}/b", "-w", "%{http_code}"]
    for hd in headers: args += ["-H", hd]
    if body is not None: open(f"{D}/req", "wb").write(body); args += ["--data-binary", f"@{D}/req"]
    return int(subprocess.run(args, capture_output=True, text=True).stdout or 0), open(f"{D}/b", "rb").read()

random.seed(20260607)
vocab = [bytes(random.choice(b"abcdefghijklmnopqrstuv") for _ in range(random.randint(3, 9))) for _ in range(300)]
def text(n): return b" ".join(random.choice(vocab) for _ in range(n)) + b"\n"
objects = {}  # (bucket, key) -> (body, headers)
def put(bucket, key, body, headers=()):
    code, resp = req("PUT", f"/{bucket}/{key}", body, list(headers))
    assert code == 201, (key, code, resp[:200]); objects[(bucket, key)] = (body, list(headers))

srv = start()
for i in range(16): put("media", f"logs/day-{i:02}.log", text(450))
stop(srv)
srv = start(NOS_RECOMPRESS_ON_STARTUP="true")
for _ in range(100):
    if os.path.exists(f"{D}/data/.dict/0.zdict"): break
    time.sleep(0.1)
time.sleep(1)
assert os.path.exists(f"{D}/data/.dict/0.zdict"), "dictionary was not trained"
put("media", "logs/with-dict.log", text(500))
twin = text(9700)
put("media", "dedup/a.txt", twin); put("media", "dedup/b.txt", twin)
put("media", "raw/random.bin", random.randbytes(6000))
put("media", "meta/typed.json", b'{"hello": "world"}\n' * 40, ["Content-Type: application/json", "x-nd-custom-meta-origin: v0.1.4"])
put("other", "logs/day-00.log", text(300))
code, b = req("POST", "/media/_multipart?key=multipart/joined.bin"); upload_id = json.loads(b)["upload_id"]
parts = [text(3000), text(10)]
for i, part in enumerate(parts, 1):
    code, b = req("PUT", f"/media/_multipart/{upload_id}/parts/{i}", part); assert code in (200, 201), (code, b)
code, b = req("POST", f"/media/_multipart/{upload_id}/complete"); assert code in (200, 201), (code, b)
objects[("media", "multipart/joined.bin")] = (b"".join(parts), [])
put("media", "trash/deleted.txt", b"deleted in v0.1.4\n")
code, _ = req("DELETE", "/media/trash/deleted.txt"); assert code in (200, 204), code
del objects[("media", "trash/deleted.txt")]
for (bucket, key), (body, _) in objects.items():
    code, got = req("GET", f"/{bucket}/{key}"); assert code == 200 and got == body, (key, code, len(got))
stop(srv)

db = sqlite3.connect(f"{D}/meta/metadata.db")
db.execute("PRAGMA wal_checkpoint(TRUNCATE)"); db.execute("PRAGMA journal_mode=DELETE"); db.execute("VACUUM"); db.close()
formats = collections.Counter()
for root, dirs, files in os.walk(f"{D}/data"):
    rel = os.path.relpath(root, f"{D}/data")
    if rel.split(os.sep)[0].startswith("."): continue
    for f in files:
        head = open(os.path.join(root, f), "rb").read(4)
        formats[head.decode() if head[:3] == b"NOS" else "raw"] += 1
shutil.rmtree(OUT, ignore_errors=True); os.makedirs(OUT)
shutil.copytree(f"{D}/data", f"{OUT}/data", ignore=shutil.ignore_patterns(".tmp", ".multipart"))
shutil.copy(f"{D}/meta/metadata.db", f"{OUT}/metadata.db")
manifest = {
    "generated_by": "nebular-os v0.1.4 (tag v0.1.4) via tests/fixtures/generate_v0_1_4.py",
    "blob_formats": dict(formats),
    "objects": [{"bucket": b, "key": k, "size": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                 "content_type": next((x.split(": ", 1)[1] for x in hd if x.startswith("Content-Type")), None),
                 "custom_meta_origin": next((x.split(": ", 1)[1] for x in hd if x.startswith("x-nd-custom-meta-origin")), None)}
                for (b, k), (body, hd) in sorted(objects.items())],
    "soft_deleted": [{"bucket": "media", "key": "trash/deleted.txt"}],
}
json.dump(manifest, open(f"{OUT}/manifest.json", "w"), indent=1)
print("formats:", dict(formats))
subprocess.run(["du", "-sh", OUT]); subprocess.run(["find", OUT, "-type", "f"], stdout=open(f"{D}/files.txt", "w"))
print(open(f"{D}/files.txt").read().count("\n"), "files")
shutil.rmtree(D)
