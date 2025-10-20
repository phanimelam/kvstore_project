# 🗄️ Build a Database: Part 1 — Simple Key–Value Store

## 📖 Overview
This project implements a **persistent key–value database** using an **append-only log** for data durability.  
It supports simple `SET`, `GET`, and `EXIT` commands, providing a minimal CLI-based interface to store and retrieve data.  
The system ensures that all data persists even after restarts by rebuilding the in-memory index from a log file (`data.db`).

---

## ⚙️ **Features**
- 🧱 Custom-built in-memory hash map (no Python dicts used)
- 💾 Append-only log-based persistence (`data.db`)
- 🔁 Data recovery after restart via log replay
- 🧠 Last-write-wins semantics for key updates
- 🧩 Modular structure:
  - Logging system (`kvstore.log`)
  - Hash table (`SimpleHashMap`)
  - Persistence utilities
  - CLI command handling
- 🧰 Fully type-hinted and documented with Google-style docstrings

---

## 🧑‍💻 **Commands**
| Command | Description |
|----------|--------------|
| `SET <key> <value>` | Stores the key-value pair persistently. |
| `GET <key>` | Retrieves the value for the given key (if exists). |
| `EXIT` | Gracefully shuts down the program. |

**Example Usage:**
```bash
SET color blue
GET color
EXIT
