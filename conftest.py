"""Opt-in time-of-day pin for the generator tests.

Generators scale their output by ``business_load()`` (0.15 at night and on weekends, up to
1.0 on weekday afternoons), so a test that passes at one hour can fail at another. Set
``GENERATOR_TEST_LOAD`` to run the whole suite at a fixed load, for example::

    GENERATOR_TEST_LOAD=0.15 python -m pytest   # weekend / night
    GENERATOR_TEST_LOAD=1.0 python -m pytest    # weekday peak

CI runs both; unset, the tests use the wall clock as before.
"""
import os
import sys

import pytest

_PIN = os.environ.get("GENERATOR_TEST_LOAD")


@pytest.fixture(autouse=True)
def _pin_business_load(monkeypatch):
    if not _PIN:
        yield
        return
    load = float(_PIN)

    def pinned(now=None):
        return load

    # Generators import business_load by name, so every importing module is patched.
    import generator_engine.db_common  # noqa: F401  (ensure the source module is loaded)

    for name, module in list(sys.modules.items()):
        if name.startswith(("generators.", "generator_engine.")) and hasattr(module, "business_load"):
            monkeypatch.setattr(module, "business_load", pinned)
    yield
