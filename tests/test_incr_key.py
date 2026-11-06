"""incr_key must survive a scalar state init (playforge payment-service crash)."""
from app.services.telemetry_dsl import YamlService


def _svc(state):
    svc = YamlService.__new__(YamlService)
    svc._state = state
    svc._resolve = lambda spec, env: spec
    return svc


def test_incr_key_scalar_state_does_not_crash():
    svc = _svc({"_refund_count": 0})
    env = {}
    svc._step_incr_key({"state_key": "_refund_count", "key": "USD", "by": 1}, env)
    svc._step_incr_key({"state_key": "_refund_count", "key": "USD", "by": 2}, env)
    assert svc._state["_refund_count"] == {"USD": 3}


def test_incr_key_dict_state():
    svc = _svc({"c": {"a": 1}})
    svc._step_incr_key({"state_key": "c", "key": "a", "by": 1}, {})
    assert svc._state["c"] == {"a": 2}
