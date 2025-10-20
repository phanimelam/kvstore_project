# Build a Database: Part 1 — Simple Key–Value Store (Append-Only)

This repository contains a minimal, persistent key–value store that supports:

```
SET <key> <value>
GET <key>
EXIT
```

- **Persistence:** All writes are appended to `data.db` with `flush()` + `fsync()` to ensure durability.
- **Recovery:** On startup, the program replays the log and rebuilds an **in-memory index** (a tiny, custom open‑addressing hash map — no built‑in dict/map used).
- **Last write wins:** Later `SET`s overwrite earlier values for the same key.

> Note: For simplicity, values are treated as single tokens (no spaces).

## How to run

Requirements:
- Python 3.9+
- No third‑party libraries required.

Run:
```bash
python kvstore.py
```
Then interact via stdin/stdout, e.g.:
```
SET a 1
OK
GET a
1
EXIT
```

## Files
- `kvstore.py` — main program (CLI + storage + in-memory index)
- `data.db` — append‑only log file created at runtime

## Implementation Notes

### Append‑only log
- Each `SET k v` is appended as a single line to `data.db`.
- After each append, we `flush()` and `os.fsync()` to persist to disk.

### Index (no dict/map)
- Uses `SimpleHashMap`, a small open‑addressing hash table implemented with lists.
- Linear probing, auto‑grows past 0.7 load factor.
- Keys and values are strings; no delete is needed for Project 1.

### CLI Protocol
- `SET <key> <value>` → prints `OK` on success.
- `GET <key>` → prints `<value>` or `NULL` if missing.
- `EXIT` → exits cleanly.

### Testing Manually
```bash
printf "SET x 10\nGET x\nEXIT\n" | python kvstore.py
```

## Using Gradebot (Black‑box tests)

1. Open Gradebot and select **test suite**: `project-1`.
2. Set **work directory** to the folder containing `kvstore.py`.
3. Set **command to run** to:
   ```
   python kvstore.py
   ```
4. Run tests. Gradebot will pipe commands to stdin and read stdout.
5. Take a screenshot of the rubric table output and save it in the repo as `gradebot_screenshot.png`.
   - Include identifying info (e.g., your EUID) *in* the screenshot.

## Git Basics

Initialize repo and make your first commit:
```bash
git init
git add kvstore.py README.md
git commit -m "Project 1: simple append-only KV store (no dict/map)"
```

Tag the final working version:
```bash
git tag project-1
git push origin main --tags
```

## Notes / Limitations

- Values cannot contain spaces in this minimal CLI format.
- The index is in-memory; the durable source of truth is the append-only log.
- For Project 2, replacing the index with a B+ Tree will make range queries and compaction easier.
