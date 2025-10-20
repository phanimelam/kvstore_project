#!/usr/bin/env python3
"""
kvstore.py
-----------
A simple persistent key–value store using append-only storage (data.db).

Features:
- Supports SET, GET, and EXIT commands.
- Uses a custom in-memory hash map (SimpleHashMap) instead of dict/map.
- Ensures durability by appending to disk and fsyncing after each SET.
- Automatically rebuilds data on startup (last-write-wins semantics).
"""

import os
import sys
import logging
from typing import Iterator, Optional, List

# --------------------------------------------------------------------
# Logging Configuration
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("kvstore.log", mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stderr),
    ],
)

DATA_FILE: str = "data.db"


def log_error(context: str, error: Exception) -> None:
    """
    Log an error message with context for debugging and tracking.

    Args:
        context (str): Description of where the error occurred.
        error (Exception): The exception that was raised.

    Returns:
        None

    Raises:
        None
    """
    logging.error(f"[{context}] {type(error).__name__}: {error}")


# --------------------------------------------------------------------
# Data Structure: SimpleHashMap
# --------------------------------------------------------------------
class SimpleHashMap:
    """
    A lightweight open-addressing hash map for string→string pairs.

    Design Highlights:
        - Linear probing for collision resolution.
        - Automatically grows when load factor > 0.7.
        - Stores keys, values, and occupancy states in parallel arrays.
    """

    __slots__ = ("_capacity", "_size", "_keys", "_vals", "_state")

    def __init__(self, initial_capacity: int = 8) -> None:
        """
        Initialize the hash table.

        Args:
            initial_capacity (int): Starting capacity of the hash table.

        Returns:
            None

        Raises:
            ValueError: Raised if the provided capacity is less than 1.
        """
        if initial_capacity < 1:
            raise ValueError("Initial capacity must be >= 1")
        cap = 1
        while cap < initial_capacity:
            cap <<= 1
        self._capacity: int = cap
        self._size: int = 0
        self._keys: List[Optional[str]] = [None] * cap
        self._vals: List[Optional[str]] = [None] * cap
        self._state: List[int] = [0] * cap

    def _hash(self, key: str) -> int:
        """
        Compute a non-negative hash index for the given key.

        Args:
            key (str): The key to hash.

        Returns:
            int: Hash index within the current table capacity.

        Raises:
            None
        """
        return (hash(key) & 0x7FFFFFFF) % self._capacity

    def _probe(self, idx: int) -> Iterator[int]:
        """
        Generate probe indices for linear probing.

        Args:
            idx (int): Starting index.

        Yields:
            int: Next index to check during probing.

        Raises:
            None
        """
        cap = self._capacity
        while True:
            yield idx
            idx = (idx + 1) % cap

    def _needs_grow(self) -> bool:
        """
        Determine whether the table should grow.

        Returns:
            bool: True if load factor exceeds 0.7.

        Raises:
            None
        """
        return self._size * 10 >= self._capacity * 7

    def _grow(self) -> None:
        """
        Double capacity and reinsert all existing entries.

        Returns:
            None

        Raises:
            None
        """
        old_keys, old_vals, old_state = self._keys, self._vals, self._state
        self._capacity <<= 1
        self._keys = [None] * self._capacity
        self._vals = [None] * self._capacity
        self._state = [0] * self._capacity
        self._size = 0
        for i in range(len(old_keys)):
            if old_state[i] == 1 and old_keys[i] is not None:
                self.put(old_keys[i], old_vals[i])

    def put(self, key: str, value: str) -> None:
        """
        Insert or update a key-value pair (last-write-wins).

        Args:
            key (str): The key to insert or update.
            value (str): The associated value.

        Returns:
            None

        Raises:
            None
        """
        if self._needs_grow():
            self._grow()
        start = self._hash(key)
        for idx in self._probe(start):
            if self._state[idx] == 0:
                self._keys[idx], self._vals[idx], self._state[idx] = key, value, 1
                self._size += 1
                return
            if self._keys[idx] == key:
                self._vals[idx] = value
                return

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve the value for a given key.

        Args:
            key (str): The key to look up.

        Returns:
            Optional[str]: The value if found, otherwise None.

        Raises:
            None
        """
        start = self._hash(key)
        for idx in self._probe(start):
            st = self._state[idx]
            if st == 0:
                return None
            if st == 1 and self._keys[idx] == key:
                return self._vals[idx]
        return None


# --------------------------------------------------------------------
# Persistence Utilities
# --------------------------------------------------------------------
def ensure_data_file(path: str) -> None:
    """
    Ensure the data file exists; create it if missing.

    Args:
        path (str): File path for the database.

    Returns:
        None

    Raises:
        OSError: Raised when the program lacks permission or disk space
                 to create the file.
    """
    if not os.path.exists(path):
        try:
            with open(path, "ab"):
                pass
            logging.info(f"Created new data file: {path}")
        except OSError as e:
            log_error("File Creation", e)
            raise


def replay_log(index: SimpleHashMap, path: str) -> None:
    """
    Replay the append-only log file to rebuild the in-memory index.

    Args:
        index (SimpleHashMap): The in-memory key-value index to update.
        path (str): Path to the data file to read from.

    Returns:
        None

    Raises:
        OSError: Raised when the log file cannot be opened or read.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) == 3 and parts[0] == "SET":
                    index.put(parts[1], parts[2])
        logging.info(f"Replayed log from '{path}' successfully.")
    except FileNotFoundError:
        logging.warning(f"No data file found at '{path}', starting fresh.")
    except OSError as e:
        log_error("Replay Log", e)
        raise


def append_set(path: str, key: str, value: str) -> None:
    """
    Append a single SET command to the log file and flush immediately.

    Args:
        path (str): Path to the data file.
        key (str): The key being stored.
        value (str): The value associated with the key.

    Returns:
        None

    Raises:
        OSError: Raised when writing to the data file fails due to
                 disk error or permission issues.
    """
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())
        logging.info(f"Persisted SET for key '{key}'")
    except OSError as e:
        log_error(f"Write Key '{key}'", e)
        raise


# --------------------------------------------------------------------
# CLI Handlers
# --------------------------------------------------------------------
def handle_set(index: SimpleHashMap, data_path: str, parts: List[str]) -> None:
    """
    Handle the SET command: validate, persist, and update memory.

    Args:
        index (SimpleHashMap): In-memory key-value index to update.
        data_path (str): Path to data file for persistence.
        parts (List[str]): Parsed command list [SET, key, value].

    Returns:
        None

    Raises:
        ValueError: Raised if the command format is invalid.
        OSError: Raised if persistence to disk fails.
    """
    if len(parts) != 3:
        raise ValueError("Invalid SET syntax. Expected: SET <key> <value>")
    key, value = parts[1], parts[2]
    append_set(data_path, key, value)
    index.put(key, value)
    logging.info(f"OK: SET '{key}' = '{value}'")


def handle_get(index: SimpleHashMap, parts: List[str]) -> None:
    """
    Handle the GET command: retrieve and print the value for a given key.

    Args:
        index (SimpleHashMap): In-memory key-value index to query.
        parts (List[str]): Parsed command list [GET, key].

    Returns:
        None

    Raises:
        ValueError: Raised if the command format is invalid.
    """
    if len(parts) != 2:
        raise ValueError("Invalid GET syntax. Expected: GET <key>")
    key = parts[1]
    val = index.get(key)
    if val is None:
        logging.info(f"GET '{key}' → [Not Found]")
    else:
        logging.info(f"GET '{key}' → '{val}'")
        print(val, flush=True)


def process_command(index: SimpleHashMap, data_path: str, line: str) -> bool:
    """
    Parse and process a single command.

    Args:
        index (SimpleHashMap): The in-memory key-value index.
        data_path (str): Path to the persistent data file.
        line (str): Raw input command line.

    Returns:
        bool: False if EXIT command issued, True otherwise.

    Raises:
        ValueError: Raised for invalid command syntax.
    """
    parts = line.split(" ", 2)
    cmd = parts[0].upper()

    if cmd == "EXIT":
        logging.info("Received EXIT command. Shutting down.")
        return False
    elif cmd == "SET":
        handle_set(index, data_path, parts)
    elif cmd == "GET":
        handle_get(index, parts)
    else:
        raise ValueError(f"Unknown command: {cmd}")
    return True


def run_cli() -> None:
    """
    Run the main command-line interface for the key–value store.

    Returns:
        None

    Raises:
        Exception: Raised if unexpected runtime errors occur during execution.
    """
    data_path = DATA_FILE
    ensure_data_file(data_path)
    index = SimpleHashMap(initial_capacity=16)
    replay_log(index, data_path)

    logging.info("Key-Value Store started. Awaiting commands...")

    try:
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            if not process_command(index, data_path, line):
                break
    except Exception as e:
        log_error("Runtime Error", e)
        raise


if __name__ == "__main__":
    run_cli()
