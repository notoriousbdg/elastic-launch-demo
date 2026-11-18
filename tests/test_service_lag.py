"""service_lag_seconds: a dependent's symptoms start after the component at fault."""

from __future__ import annotations

from app.services.base_service import BaseService


class _Controller:
    def __init__(self, elapsed: float | None):
        self._elapsed = elapsed

    def get_channel_elapsed(self, channel: int):
        return self._elapsed


class _Stub(BaseService):
    """Concrete enough to call the onset helpers without building a scenario."""

    def generate_telemetry(self, *args, **kwargs):  # pragma: no cover - never called
        raise NotImplementedError


def _service(name: str, elapsed: float | None) -> BaseService:
    svc = object.__new__(_Stub)
    svc.SERVICE_NAME = name
    svc.chaos_controller = _Controller(elapsed)
    return svc


CH = {"onset_delay_seconds": 10, "service_lag_seconds": {"dependent": 30}}


def test_service_without_a_lag_entry_uses_the_channel_onset():
    assert not _service("root", 5)._onset_reached(1, CH)
    assert _service("root", 10)._onset_reached(1, CH)


def test_lagged_service_waits_onset_plus_lag():
    assert not _service("dependent", 39)._onset_reached(1, CH)
    assert _service("dependent", 40)._onset_reached(1, CH)


def test_lag_alone_delays_a_channel_without_onset():
    ch = {"service_lag_seconds": {"dependent": 20}}
    assert _service("root", 0)._onset_reached(1, ch)
    assert not _service("dependent", 19)._onset_reached(1, ch)
    assert _service("dependent", 20)._onset_reached(1, ch)


def test_bad_lag_values_are_ignored():
    for bad in (-5, "soon", None):
        ch = {"service_lag_seconds": {"dependent": bad}}
        assert _service("dependent", 0)._onset_reached(1, ch)
