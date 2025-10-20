#!/usr/bin/env python3
# kvstore.py
# A simple persistent key–value store using append-only log (data.db)
# No built-in dict/map used for the in-memory index.
#
# Commands (stdin/stdout):
#   SET <key> <value>
#   GET <key>
#   EXIT
#
# Persistence:
#   - Appends "SET <key> <value>\n" to data.db.
#   - Flush + fsync after each SET for durability.
#   - On startup, replays data.db to rebuild in-memory index.
#
# In-Memory Index:
#   - A tiny open-addressing hash table implemented with Python lists.
#   - Keys and values are strings.
#
# Last-write-wins:
#   - Replay applies SETs in order; later writes overwrite earlier ones.

import os
import sys

DATA_FILE = "data.db"

class SimpleHashMap:
    """
    A very small open-addressing hash table for string->string without using dict.
    - Linear probing
    - Grows when load factor exceeds 0.7
    - Keys, values, and states are parallel arrays
    States: 0 = empty, 1 = occupied
    (We don't implement delete for this project.)
    """
    __slots__ = ("_capacity", "_size", "_keys", "_vals", "_state")

    def __init__(self, initial_capacity=8):
        cap = 1
        while cap < initial_capacity:
            cap <<= 1
        self._capacity = cap
        self._size = 0
        self._keys = [None] * self._capacity
        self._vals = [None] * self._capacity
        self._state = [0] * self._capacity

    def _hash(self, key: str) -> int:
        # Use Python's hash but mask to positive
        return (hash(key) & 0x7FFFFFFF) % self._capacity

    def _probe(self, idx: int):
        cap = self._capacity
        while True:
            yield idx
            idx = (idx + 1) % cap

    def _needs_grow(self) -> bool:
        return self._size * 10 >= self._capacity * 7  # load factor > 0.7

    def _grow(self):
        old_keys = self._keys
        old_vals = self._vals
        old_state = self._state
        self._capacity <<= 1
        self._keys = [None] * self._capacity
        self._vals = [None] * self._capacity
        self._state = [0] * self._capacity
        self._size = 0
        for i in range(len(old_keys)):
            if old_state[i] == 1:
                self.put(old_keys[i], old_vals[i])

    def put(self, key: str, value: str):
        if self._needs_grow():
            self._grow()
        start = self._hash(key)
        for idx in self._probe(start):
            if self._state[idx] == 0:
                # empty slot
                self._keys[idx] = key
                self._vals[idx] = value
                self._state[idx] = 1
                self._size += 1
                return
            # occupied
            if self._keys[idx] == key:
                self._vals[idx] = value
                return

    def get(self, key: str):
        start = self._hash(key)
        for idx in self._probe(start):
            st = self._state[idx]
            if st == 0:
                # hit an empty slot; key not present
                return None
            if st == 1 and self._keys[idx] == key:
                return self._vals[idx]

def ensure_data_file(path: str):
    if not os.path.exists(path):
        # Create empty file
        with open(path, "ab"):
            pass

def replay_log(index: SimpleHashMap, path: str):
    # Read line by line; apply SET commands
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Format: SET <key> <value>
                # Value cannot contain spaces for this project.
                # Ignore lines that don't parse.
                parts = line.split(" ", 2)
                if len(parts) != 3:
                    continue
                cmd, key, value = parts
                if cmd != "SET":
                    continue
                index.put(key, value)
    except FileNotFoundError:
        # Nothing to replay
        pass

def append_set(path: str, key: str, value: str):
    # Append a single-line record and fsync for durability
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"SET {key} {value}\n")
        f.flush()
        os.fsync(f.fileno())

def run_cli():
    data_path = DATA_FILE
    ensure_data_file(data_path)
    index = SimpleHashMap(initial_capacity=16)
    replay_log(index, data_path)

    # Read-eval-print loop
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        # Commands: SET <key> <value> | GET <key> | EXIT
        parts = line.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "EXIT":
            # graceful shutdown
            return

        elif cmd == "SET":
            if len(parts) != 3:
                print("ERR", flush=True)
                continue
            key = parts[1]
            value = parts[2]
            try:
                append_set(data_path, key, value)
                index.put(key, value)
                print("OK", flush=True)
            except Exception:
                print("ERR", flush=True)

        elif cmd == "GET":
            if len(parts) != 2:
                print("ERR", flush=True)
                continue
            key = parts[1]
            val = index.get(key)
            if val is None:
                print("NULL", flush=True)
            else:
                print(val, flush=True)
        else:
            print("ERR", flush=True)

if __name__ == "__main__":
    # If the script is invoked directly, run the CLI.
    run_cli()
