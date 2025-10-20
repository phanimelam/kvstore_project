"""
test_kvstore.py
---------------
Unit tests for the Build a Database: Part 1 – Simple Key–Value Store project.

Author:
    Phani Melam
    Master's in Computer Science

Description:
    These test cases verify the core functionality of kvstore.py, including
    insertion, retrieval, overwriting, persistence, and edge-case handling.
"""

import os
import pytest
from kvstore import SimpleHashMap, append_set, replay_log


def test_put_and_get():
    """Test that a key can be added and retrieved successfully."""
    kv = SimpleHashMap()
    kv.put("name", "Phani")
    assert kv.get("name") == "Phani"


def test_overwrite_key():
    """Test that overwriting an existing key updates its value correctly."""
    kv = SimpleHashMap()
    kv.put("language", "Python")
    kv.put("language", "Go")
    assert kv.get("language") == "Go"


def test_replay_log(tmp_path):
    """Test that log replay restores data correctly from the persistent file."""
    data_file = tmp_path / "data.db"
    append_set(data_file, "fruit", "mango")
    index = SimpleHashMap()
    replay_log(index, data_file)
    assert index.get("fruit") == "mango"


def test_empty_get_returns_none():
    """Test that getting a non-existent key returns None."""
    kv = SimpleHashMap()
    assert kv.get("does_not_exist") is None


def test_multiple_puts():
    """Test multiple key-value pairs for correctness."""
    kv = SimpleHashMap()
    kv.put("key1", "val1")
    kv.put("key2", "val2")
    kv.put("key3", "val3")
    assert kv.get("key1") == "val1"
    assert kv.get("key2") == "val2"
    assert kv.get("key3") == "val3"


# ---------------- EDGE CASES ----------------

def test_long_key_and_value():
    """Test insertion of a long key and value pair."""
    kv = SimpleHashMap()
    long_key = "k" * 200
    long_value = "v" * 500
    kv.put(long_key, long_value)
    assert kv.get(long_key) == long_value


def test_special_characters_in_key_value():
    """Test keys and values containing special characters."""
    kv = SimpleHashMap()
    kv.put("spécial@key!", "välüe#123$")
    assert kv.get("spécial@key!") == "välüe#123$"


def test_duplicate_puts_same_value():
    """Test putting the same value repeatedly for a key."""
    kv = SimpleHashMap()
    kv.put("color", "blue")
    kv.put("color", "blue")
    assert kv.get("color") == "blue"
