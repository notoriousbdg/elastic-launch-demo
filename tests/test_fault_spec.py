"""Unit tests for scenario_engine/fault_spec.py DSL resolver."""

from __future__ import annotations

import random
import re

import pytest

from scenario_engine.fault_spec import resolve, resolve_dict


# Seed for deterministic test assertions.
SEED = 42


def rng(seed: int = SEED) -> random.Random:
    return random.Random(seed)


# ---------------------------------------------------------------------------
# Scalars and non-DSL values
# ---------------------------------------------------------------------------

class TestPassThrough:
    def test_string(self):
        assert resolve("hello") == "hello"

    def test_int(self):
        assert resolve(42) == 42

    def test_float(self):
        assert resolve(3.14) == 3.14

    def test_bool_true(self):
        assert resolve(True) is True

    def test_bool_false(self):
        assert resolve(False) is False

    def test_none(self):
        assert resolve(None) is None

    def test_list_passthrough(self):
        lst = [1, "two", 3.0]
        assert resolve(lst) is lst

    def test_plain_dict_no_dsl_key(self):
        d = {"foo": "bar", "baz": 99}
        assert resolve(d) is d


# ---------------------------------------------------------------------------
# choice
# ---------------------------------------------------------------------------

class TestChoice:
    def test_from_strings(self):
        r = rng()
        result = resolve({"choice": ["a", "b", "c"]}, r)
        assert result in {"a", "b", "c"}

    def test_from_ints(self):
        r = rng()
        result = resolve({"choice": [100, 200, 300]}, r)
        assert result in {100, 200, 300}

    def test_from_bools(self):
        r = rng()
        result = resolve({"choice": [True, False]}, r)
        assert isinstance(result, bool)

    def test_single_element(self):
        r = rng()
        assert resolve({"choice": ["only"]}, r) == "only"

    def test_repeats_same_element(self):
        # A list with duplicates should be respected.
        r = rng()
        result = resolve({"choice": ["normal", "normal", "elevated"]}, r)
        assert result in {"normal", "elevated"}

    def test_deterministic_with_seed(self):
        a = resolve({"choice": ["x", "y", "z"]}, rng(1))
        b = resolve({"choice": ["x", "y", "z"]}, rng(1))
        assert a == b


# ---------------------------------------------------------------------------
# randint
# ---------------------------------------------------------------------------

class TestRandint:
    def test_within_range(self):
        r = rng()
        result = resolve({"randint": [1, 10]}, r)
        assert 1 <= result <= 10
        assert isinstance(result, int)

    def test_equal_bounds(self):
        r = rng()
        assert resolve({"randint": [5, 5]}, r) == 5

    def test_large_range(self):
        r = rng()
        result = resolve({"randint": [100000, 999999]}, r)
        assert 100000 <= result <= 999999


# ---------------------------------------------------------------------------
# uniform
# ---------------------------------------------------------------------------

class TestUniform:
    def test_within_range(self):
        r = rng()
        result = resolve({"uniform": [0.0, 1.0]}, r)
        assert 0.0 <= result <= 1.0
        assert isinstance(result, float)

    def test_round_modifier(self):
        r = rng()
        result = resolve({"uniform": [80.0, 99.9], "round": 1}, r)
        # After round(x, 1) the string representation has at most 1 decimal.
        assert result == round(result, 1)

    def test_round_zero(self):
        r = rng()
        result = resolve({"uniform": [0.0, 100.0], "round": 0}, r)
        # round(x, 0) returns a float with 0 decimal places.
        assert result == round(result, 0)

    def test_no_round_returns_float(self):
        r = rng()
        result = resolve({"uniform": [1.0, 2.0]}, r)
        assert isinstance(result, float)


# ---------------------------------------------------------------------------
# random
# ---------------------------------------------------------------------------

class TestRandom:
    def test_within_01(self):
        r = rng()
        result = resolve({"random": True}, r)
        assert 0.0 <= result <= 1.0

    def test_round_modifier(self):
        r = rng()
        result = resolve({"random": True, "round": 2}, r)
        assert result == round(result, 2)


# ---------------------------------------------------------------------------
# format
# ---------------------------------------------------------------------------

class TestFormat:
    def test_simple_substitution(self):
        r = rng()
        result = resolve(
            {"format": "ORD-{n}", "n": {"randint": [1000, 9999]}},
            r,
        )
        assert re.match(r"^ORD-\d{4}$", result), repr(result)

    def test_hex_format_spec(self):
        r = rng()
        result = resolve(
            {"format": "{n:02x}", "n": {"randint": [0, 255]}},
            r,
        )
        assert re.match(r"^[0-9a-f]{2}$", result), repr(result)

    def test_uppercase_hex(self):
        r = rng()
        result = resolve(
            {"format": "{n:X}", "n": {"randint": [100000, 999999]}},
            r,
        )
        assert re.match(r"^[0-9A-F]+$", result), repr(result)

    def test_mac_address(self):
        r = rng()
        result = resolve(
            {
                "format": "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{f:02x}",
                "a": {"randint": [0, 255]},
                "b": {"randint": [0, 255]},
                "c": {"randint": [0, 255]},
                "d": {"randint": [0, 255]},
                "e": {"randint": [0, 255]},
                "f": {"randint": [0, 255]},
            },
            r,
        )
        assert re.match(r"^([0-9a-f]{2}:){5}[0-9a-f]{2}$", result), repr(result)

    def test_ip_address(self):
        r = rng()
        result = resolve(
            {
                "format": "10.{a}.{b}.{c}",
                "a": {"randint": [0, 255]},
                "b": {"randint": [0, 255]},
                "c": {"randint": [1, 254]},
            },
            r,
        )
        parts = result.split(".")
        assert parts[0] == "10"
        assert all(p.isdigit() for p in parts)

    def test_nested_choice_in_format(self):
        r = rng()
        result = resolve(
            {
                "format": "sess-{tier}-{n}",
                "tier": {"choice": ["free", "pro", "enterprise"]},
                "n": {"randint": [1, 999]},
            },
            r,
        )
        assert result.startswith("sess-")

    def test_static_suffix(self):
        r = rng()
        result = resolve({"format": "PREFIX-{n}-SUFFIX", "n": {"randint": [0, 9]}}, r)
        assert result.startswith("PREFIX-")
        assert result.endswith("-SUFFIX")


# ---------------------------------------------------------------------------
# resolve_dict
# ---------------------------------------------------------------------------

class TestResolveDict:
    def test_mixed_specs(self):
        r = rng()
        specs = {
            "static": "hello",
            "count": {"randint": [1, 10]},
            "pct": {"uniform": [0.0, 100.0], "round": 1},
        }
        result = resolve_dict(specs, r)
        assert result["static"] == "hello"
        assert 1 <= result["count"] <= 10
        assert 0.0 <= result["pct"] <= 100.0

    def test_empty_dict(self):
        assert resolve_dict({}) == {}

    def test_uses_shared_rng(self):
        """Same RNG is threaded through all keys so sequence is reproducible."""
        r1 = rng(7)
        r2 = rng(7)
        specs = {"a": {"randint": [0, 100]}, "b": {"randint": [0, 100]}}
        assert resolve_dict(specs, r1) == resolve_dict(specs, r2)


# ---------------------------------------------------------------------------
# None rng — should create its own
# ---------------------------------------------------------------------------

class TestDefaultRng:
    def test_none_rng_creates_fresh(self):
        # Two calls with rng=None should produce valid results (may differ).
        r1 = resolve({"randint": [0, 1000000]}, None)
        r2 = resolve({"randint": [0, 1000000]}, None)
        assert 0 <= r1 <= 1000000
        assert 0 <= r2 <= 1000000
