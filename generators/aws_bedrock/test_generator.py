"""Bedrock-specific field-shape tests for dashboard grounding."""

import random

from generators.aws_bedrock.generator import (
    DOC_FIELDS,
    _CONTENT_SOURCES,
    _GUARDRAIL_POLICY_TYPES,
    _build_guardrail_doc,
    _build_guardrail_docs,
    _build_invocation_doc,
    _build_runtime_doc,
    _error_quota,
    _error_rate,
)


_PROFILE = {
    "aws_account_id": "112233445566",
    "aws_account_name": "Banking Demo AWS",
    "aws_region": "us-east-1",
    "bedrock_models": [
        "us.anthropic.claude-sonnet-5",
        "amazon.titan-image-generator-v2:0",
    ],
    "bedrock_guardrail_id": "banking-pii-compliance-guardrail",
    "bedrock_guardrail_arn": "arn:aws:bedrock:us-east-1:112233445566:guardrail/x",
    "bedrock_guardrail_version": "1",
    "prompt_templates": ["Hello?"],
    "response_templates": ["Hi."],
    "callers": [{"email": "a@b.c", "country": "US", "city": "NYC", "lat": 1.0, "lon": 2.0}],
}


def test_invocation_doc_matches_package_fields():
    doc = _build_invocation_doc(1_700_000_000.0, "banking", _PROFILE, random.Random(1), None)
    assert doc["cloud"]["account"]["name"] == "Banking Demo AWS"
    assert doc["gen_ai"]["request"]["model"]["id"]
    assert "prompt_tokens" in doc["gen_ai"]["usage"]
    assert "completion_tokens" in doc["gen_ai"]["usage"]
    assert "response_time" in doc["gen_ai"]["performance"]
    assert doc["aws_bedrock"]["invocation"]["request_id"]
    assert "model_id" in doc["aws_bedrock"]["invocation"]


def test_runtime_doc_includes_token_and_server_error_fields():
    doc = _build_runtime_doc(
        1_700_000_000.0, "banking", _PROFILE, "us.anthropic.claude-sonnet-5",
        random.Random(2), 40, None,
    )
    rt = doc["aws_bedrock"]["runtime"]
    assert doc["cloud"]["account"]["name"] == "Banking Demo AWS"
    assert "invocation_server_errors" in rt
    assert "input_token_count" in rt
    assert "output_token_count" in rt
    assert "output_image_count" in rt
    assert rt["model_id"] == "us.anthropic.claude-sonnet-5"
    # emptyAsNull maps sum(0) → null; healthy baseline must be non-zero.
    assert 1 <= rt["invocation_throttles"] <= 3


def test_runtime_throttles_rise_under_chaos():
    doc = _build_runtime_doc(
        1_700_000_000.0, "banking", _PROFILE, "us.anthropic.claude-sonnet-5",
        random.Random(2), 40, {"throttle_multiplier": 2.0},
    )
    assert doc["aws_bedrock"]["runtime"]["invocation_throttles"] >= 5


def test_image_model_runtime_emits_output_images():
    doc = _build_runtime_doc(
        1_700_000_000.0, "banking", _PROFILE, "amazon.titan-image-generator-v2:0",
        random.Random(3), 10, None,
    )
    rt = doc["aws_bedrock"]["runtime"]
    assert rt["output_image_count"] > 0
    assert rt["model_id"] == "amazon.titan-image-generator-v2:0"
    # Generated-images panels require quality/image_size to be absent.
    assert "quality" not in rt
    assert "image_size" not in rt


def test_text_model_runtime_omits_image_dimensions():
    doc = _build_runtime_doc(
        1_700_000_000.0, "banking", _PROFILE, "us.anthropic.claude-sonnet-5",
        random.Random(7), 10, None,
    )
    rt = doc["aws_bedrock"]["runtime"]
    assert "quality" not in rt
    assert "image_size" not in rt
    assert "bucketed_step_size" not in rt


def test_error_invocation_sets_error_code():
    doc = _build_invocation_doc(
        1_700_000_000.0, "banking", _PROFILE, random.Random(1), None, force_error=True
    )
    assert doc["event"]["outcome"] == "failure"
    inv = doc["aws_bedrock"]["invocation"]
    assert inv.get("error_code")
    assert inv.get("error") == inv["error_code"]


def test_success_invocation_omits_error_code():
    # force_error=False + seed that lands on success
    found = None
    for seed in range(400):
        doc = _build_invocation_doc(
            1_700_000_000.0, "banking", _PROFILE, random.Random(seed), None
        )
        if doc.get("event", {}).get("outcome") == "success":
            found = doc
            break
    assert found is not None
    assert "error_code" not in found["aws_bedrock"]["invocation"]
    assert "error" not in found["aws_bedrock"]["invocation"]


def test_error_quota_is_about_12_percent_min_one():
    assert _error_rate(None) == 0.12
    assert _error_quota(1, None) == 1
    assert _error_quota(8, None) == 1
    assert _error_quota(100, None) == 12


def test_batch_errors_carry_error_code_only_on_failures():
    """Per-batch ~12% (min 1) failures; success rows omit error_code."""
    n = 50
    rng = random.Random(11)
    err_quota = _error_quota(n, None)
    err_slots = set(rng.sample(range(n), min(err_quota, n)))
    docs = [
        _build_invocation_doc(
            1_700_000_000.0, "banking", _PROFILE, random.Random(100 + i), None,
            force_error=(i in err_slots),
        )
        for i in range(n)
    ]
    failures = [d for d in docs if d["event"]["outcome"] == "failure"]
    successes = [d for d in docs if d["event"]["outcome"] == "success"]
    assert len(failures) >= err_quota
    assert all(d["aws_bedrock"]["invocation"].get("error_code") for d in failures)
    assert all("error_code" not in d["aws_bedrock"]["invocation"] for d in successes)


def test_guardrail_doc_includes_latency_and_account_name():
    doc = _build_guardrail_doc(
        1_700_000_000.0, "banking", _PROFILE, random.Random(4), 3, 40, None,
        content_source="Input",
        policy_type="CONTENT_POLICY",
    )
    gr = doc["aws_bedrock"]["guardrails"]
    assert doc["cloud"]["account"]["name"] == "Banking Demo AWS"
    assert "invocation_latency" in gr
    assert gr["invocations_intervened"] == 3
    assert gr["guardrail_content_source"] == "Input"
    assert gr["guardrail_policy_type"] == "CONTENT_POLICY"
    # emptyAsNull: healthy-path throttles must be non-zero
    assert 1 <= gr["invocation_throttles"] <= 2


def test_guardrail_docs_cover_dashboard_filters():
    docs = _build_guardrail_docs(
        1_700_000_000.0, "banking", _PROFILE, random.Random(5), 4, 40, None,
    )
    assert _CONTENT_SOURCES == ("Input", "Output")
    overall = [
        d for d in docs
        if "guardrail_policy_type" not in d["aws_bedrock"]["guardrails"]
    ]
    by_policy = [
        d for d in docs
        if "guardrail_policy_type" in d["aws_bedrock"]["guardrails"]
    ]
    assert len(overall) == 2
    assert {d["aws_bedrock"]["guardrails"]["guardrail_content_source"] for d in overall} == {
        "Input",
        "Output",
    }
    assert len(by_policy) == 2 * len(_GUARDRAIL_POLICY_TYPES)
    assert all(
        d["aws_bedrock"]["guardrails"]["guardrail_content_source"] in _CONTENT_SOURCES
        for d in by_policy
    )


def test_intervened_invocation_has_violation_code_not_null():
    # Force interventions by building many docs until one intervenes.
    found = None
    for seed in range(200):
        doc = _build_invocation_doc(
            1_700_000_000.0, "banking", _PROFILE, random.Random(seed), None
        )
        compliance = doc.get("gen_ai", {}).get("compliance")
        if compliance and compliance.get("violation_detected") is True:
            found = doc
            break
    assert found is not None
    assert found["gen_ai"]["compliance"]["violation_code"]
    assert found["aws_bedrock"]["invocation"]["output"]["completion_text"]
    assert found["gen_ai"]["prompt"]


def test_guardrail_checked_docs_always_set_violation_code():
    """Discover panel filters on guardrail_id only — code must never be null."""
    found_pass = found_block = None
    for seed in range(500):
        doc = _build_invocation_doc(
            1_700_000_000.0, "banking", _PROFILE, random.Random(seed), None
        )
        compliance = doc.get("gen_ai", {}).get("compliance")
        if not compliance:
            continue
        assert compliance.get("violation_code")
        if compliance.get("violation_detected") is False and found_pass is None:
            found_pass = doc
        if compliance.get("violation_detected") is True and found_block is None:
            found_block = doc
        if found_pass and found_block:
            break
    assert found_pass is not None
    assert found_block is not None


def test_emit_namespaces_includes_ootb_default():
    from generator_engine.es_doc_common import emit_namespaces

    assert emit_namespaces("default") == ["default"]
    assert emit_namespaces("xborder") == ["xborder", "default"]


# ── aws_bedrock@2.1.0 gen_ai.* request parameters and messages ─────────────────
# The 2.x invocation pipeline maps Converse inference parameters and the request /
# response messages to ECS gen_ai fields; the generator bypasses pipelines
# (pipeline=_none), so it must write the post-pipeline shape itself.

_CHAT_MODEL = "us.anthropic.claude-sonnet-5"
_IMAGE_MODEL = "amazon.titan-image-generator-v2:0"


def _success_doc(model: str, seed: int = 1) -> dict:
    profile = {**_PROFILE, "bedrock_models": [model]}
    return _build_invocation_doc(
        1_700_000_000.0, "banking", profile, random.Random(seed), None, force_error=False,
    )


def _chat_success_docs(count: int = 40) -> list[dict]:
    docs = [_success_doc(_CHAT_MODEL, seed) for seed in range(count)]
    # No error block: a guardrail intervention can reset result to "success" on an error.
    return [d for d in docs if "error" not in d]


def test_chat_invocation_carries_inference_parameters_with_package_types():
    for doc in _chat_success_docs():
        request = doc["gen_ai"]["request"]
        assert isinstance(request["max_tokens"], int) and request["max_tokens"] > 0  # long
        assert isinstance(request["temperature"], float) and 0.0 <= request["temperature"] <= 1.0
        assert isinstance(request["top_p"], float) and 0.0 < request["top_p"] <= 1.0


def test_chat_invocation_carries_input_and_output_messages():
    docs = _chat_success_docs()
    assert docs
    for doc in docs:
        assert doc["gen_ai"]["input"]["messages"] == [
            {"role": "user", "content": [{"text": "Hello?"}]}
        ]
        assert doc["gen_ai"]["output"]["messages"] == [
            {"role": "assistant", "content": [{"text": "Hi."}]}
        ]


def test_failed_invocation_has_no_output_messages():
    doc = _build_invocation_doc(
        1_700_000_000.0, "banking", {**_PROFILE, "bedrock_models": [_CHAT_MODEL]},
        random.Random(1), None, force_error=True,
    )
    assert "messages" in doc["gen_ai"]["input"]
    assert "output" not in doc["gen_ai"]


def test_image_invocation_has_no_chat_fields():
    doc = _success_doc(_IMAGE_MODEL)
    assert "input" not in doc["gen_ai"] and "output" not in doc["gen_ai"]
    for field in ("max_tokens", "temperature", "top_p"):
        assert field not in doc["gen_ai"]["request"]


def test_doc_fields_list_the_new_gen_ai_fields():
    for field in (
        "gen_ai.request.max_tokens", "gen_ai.request.temperature", "gen_ai.request.top_p",
        "gen_ai.input.messages", "gen_ai.output.messages",
    ):
        assert field in DOC_FIELDS


def test_shared_ecs_version_is_left_alone():
    assert _success_doc(_CHAT_MODEL)["ecs"]["version"] == "8.11.0"


# --- prompt/completion follow the model type -----------------------------------------

import pytest  # noqa: E402

from generators.aws_bedrock.generator import _is_image_model  # noqa: E402

_TEXT_PROMPTS = {"Hello?"}
_TEXT_RESPONSES = {"Hi."}


def _docs(model_filter, seeds=range(400), chaos=None):
    for seed in seeds:
        doc = _build_invocation_doc(1_700_000_000.0, "banking", _PROFILE, random.Random(seed), chaos)
        if model_filter(doc["gen_ai"]["request"]["model"]["id"]):
            yield doc


def test_image_model_calls_never_carry_a_text_model_prompt():
    docs = list(_docs(_is_image_model))
    assert docs, "expected some image-model invocations in 400 seeds"
    for doc in docs:
        assert doc["gen_ai"]["prompt"] not in _TEXT_PROMPTS, doc["gen_ai"]["prompt"]
        assert doc["gen_ai"]["prompt"].lower().startswith(("generate", "create", "draw", "design", "render"))


def test_image_model_calls_have_no_text_completion():
    for doc in _docs(_is_image_model):
        output = doc["aws_bedrock"]["invocation"]["output"]
        assert "completion_text" not in output, "image calls return images, not text"
        assert "completion" not in doc["gen_ai"]


def test_text_model_calls_keep_the_scenario_prompt_and_a_completion():
    docs = list(_docs(lambda m: not _is_image_model(m)))
    assert docs
    for doc in docs:
        assert doc["gen_ai"]["prompt"] in _TEXT_PROMPTS
        assert doc["aws_bedrock"]["invocation"]["output"]["completion_text"] in _TEXT_RESPONSES
        assert doc["gen_ai"]["completion"] in _TEXT_RESPONSES


def test_same_prompt_never_appears_with_and_without_a_completion():
    """The dashboard symptom: identical prompt text, some rows with a response, some without."""
    with_completion, without = set(), set()
    for doc in _docs(lambda m: True, chaos=None):
        prompt = doc["gen_ai"]["prompt"]
        has = bool(doc["aws_bedrock"]["invocation"]["output"].get("completion_text"))
        (with_completion if has else without).add(prompt)
    assert not (with_completion & without), with_completion & without


def test_profile_can_supply_image_prompts():
    profile = {**_PROFILE, "image_prompt_templates": ["Create a branded hero banner."]}
    for seed in range(400):
        doc = _build_invocation_doc(1_700_000_000.0, "banking", profile, random.Random(seed), None)
        if _is_image_model(doc["gen_ai"]["request"]["model"]["id"]):
            assert doc["gen_ai"]["prompt"] == "Create a branded hero banner."
            return
    pytest.fail("no image-model invocation produced")
