"""Library workflows that investigate an integration's data, matched to it generically.

Integration packages ship alert rules, SLOs and ML jobs but no workflows. The Elastic
Workflow Template Library (vendored in ``elastic_config/workflows/library`` by
``scripts/snapshot_workflow_templates.py``) has workflows that investigate one
integration's data, such as ``aws-rds-connection-exhaustion-investigation-otel``.
Nothing names the integration, so the link comes from the data the workflow reads:

* its **evidence step** — the ES|QL step the evidence gate checks
  (``steps.<step>.output.documents_found``) — reads ``FROM`` an index pattern such as
  ``metrics-aws.rds.otel-*``; the other required (no ``on-failure: continue``)
  top-level ES|QL steps read data too;
* its alert path reads the resource from ``kibana.alert.grouping.<Dim>``.

A template **deploys** when it is an observability template the stack version supports,
needs no install-form input, and every required step reads a dataset that an enabled
generator writes. It is **wired** to each OOTB package rule that reads the evidence
step's data, groups by ``<Dim>`` and reads at least one metric the evidence step reads.
Any library workflow with that shape is picked up the same way.
"""
from __future__ import annotations

import fnmatch
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

LIBRARY_DIR = Path(__file__).resolve().parent / "workflows" / "library"
OOTB_WORKFLOW_TAG = "ootb-workflow"
_SIGNALS = ("metrics", "logs", "traces")

_CONST = re.compile(r"\{\{\s*consts\.([A-Za-z_][\w]*)\s*\}\}")
_FROM = re.compile(r"^\s*(?:FROM|TS)\s+([^\s|]+(?:\s*,\s*[^\s|]+)*)", re.I | re.M)
_EVIDENCE = re.compile(r"steps\.([A-Za-z_][\w]*)\.output\.documents_found")
_GROUPING = re.compile(r"kibana\.alert\.grouping\.([A-Za-z_][\w.]*)")
_METRIC_NAME_EQ = re.compile(r"""MetricName\s*==\s*"([^"]+)\"""")
_METRIC_NAME_IN = re.compile(r"""MetricName\s+IN\s*\(([^)]*)\)""", re.I)
_METRIC_FIELD = re.compile(r"`?metrics\.([^`\s,)|]+)`?")
_BY = re.compile(r"\bBY\s+(.+?)(?:\n\s*\||\n\s*//|$)", re.S)
_INPUT_FALLBACK = r"(inputs\.{name}\s*\|\s*default:\s*)'[^']*'"


@dataclass(frozen=True)
class EsqlStep:
    name: str
    query: str
    from_patterns: tuple[str, ...]
    required: bool
    top_level: bool


@dataclass
class LibraryWorkflow:
    slug: str
    name: str
    text: str
    meta: dict[str, Any]
    triggers: tuple[str, ...]
    manual_inputs: tuple[str, ...]
    steps: list[EsqlStep] = field(default_factory=list)
    evidence_step: str | None = None
    grouping: tuple[str, ...] = ()

    @property
    def solutions(self) -> list[str]:
        return list(self.meta.get("solutions") or [])

    @property
    def install_form(self) -> list[dict]:
        return list(((self.meta.get("install") or {}).get("form")) or [])

    @property
    def required_steps(self) -> list[EsqlStep]:
        return [s for s in self.steps if s.top_level and s.required and _data_patterns(s.from_patterns)]

    @property
    def evidence(self) -> EsqlStep | None:
        return next((s for s in self.steps if s.name == self.evidence_step), None)


def _data_patterns(patterns: Iterable[str]) -> list[str]:
    """Index patterns that hold integration data (not ``.alerts-*`` / ``.slo-*``)."""
    return [p for p in patterns if p.split("-", 1)[0] in _SIGNALS]


def esql_from(query: str) -> list[str]:
    match = _FROM.search(query or "")
    return [p.strip() for p in match.group(1).split(",")] if match else []


def esql_metric_names(query: str) -> set[str]:
    """CloudWatch-style metric names a query reads (``MetricName`` literals and metric fields)."""
    names = set(_METRIC_NAME_EQ.findall(query or ""))
    for group in _METRIC_NAME_IN.findall(query or ""):
        names.update(re.findall(r'"([^"]+)"', group))
    for raw in _METRIC_FIELD.findall(query or ""):
        names.add(raw.rstrip("`").rsplit("/", 1)[-1])
    return names


def esql_group_columns(query: str) -> list[str]:
    """Columns of the last ``STATS ... BY`` (the alert grouping of an ES|QL rule)."""
    found = _BY.findall(re.sub(r"//[^\n]*", "", query or ""))
    if not found:
        return []
    return [c.strip().strip("`") for c in found[-1].split(",") if c.strip()]


def _walk_steps(steps: list | None, top_level: bool, consts: dict[str, str], out: list[EsqlStep]) -> None:
    for step in steps or []:
        if not isinstance(step, dict):
            continue
        if step.get("type") == "elasticsearch.esql.query":
            query = _CONST.sub(lambda m: str(consts.get(m.group(1), m.group(0))), (step.get("with") or {}).get("query") or "")
            required = not ((step.get("on-failure") or {}).get("continue") is True)
            out.append(EsqlStep(step.get("name", ""), query, tuple(esql_from(query)), required, top_level))
        for key in ("steps", "else"):
            _walk_steps(step.get(key), False, consts, out)


def parse_workflow(text: str, slug: str) -> LibraryWorkflow:
    doc = yaml.safe_load(text) or {}
    consts = {k: str(v) for k, v in (doc.get("consts") or {}).items()}
    triggers = doc.get("triggers") or [{"type": "manual"}]
    manual_inputs = tuple(
        i.get("name") for t in triggers if t.get("type") == "manual"
        for i in (t.get("inputs") or []) if isinstance(i, dict) and i.get("name")
    )
    wf = LibraryWorkflow(
        slug=slug,
        name=str(doc.get("name") or slug),
        text=text,
        meta=doc.get("template-metadata") or {},
        triggers=tuple(t.get("type") for t in triggers if isinstance(t, dict)),
        manual_inputs=manual_inputs,
    )
    _walk_steps(doc.get("steps"), True, consts, wf.steps)
    for step in doc.get("steps") or []:
        if isinstance(step, dict) and step.get("type") == "if":
            found = _EVIDENCE.search(str(step.get("condition") or ""))
            if found:
                wf.evidence_step = found.group(1)
                break
    wf.grouping = tuple(dict.fromkeys(_GROUPING.findall(text)))
    return wf


def load_library(directory: Path = LIBRARY_DIR) -> list[LibraryWorkflow]:
    return [parse_workflow(p.read_text(), p.stem) for p in sorted(directory.glob("*.yaml"))]


def _version_tuple(text: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", text)[:3])


def availability_ok(spec: str | None, version: str | None) -> bool:
    """``>=9.5.0``-style availability against a stack version (None = serverless/latest)."""
    if not spec or not version:
        return True
    for clause in spec.split(","):
        match = re.match(r"\s*(>=|<=|>|<|==|=)?\s*([\d.]+)", clause)
        if not match:
            continue
        op, want = match.group(1) or ">=", _version_tuple(match.group(2))
        have = _version_tuple(version)
        ok = {">=": have >= want, ">": have > want, "<=": have <= want, "<": have < want,
              "==": have == want, "=": have == want}[op]
        if not ok:
            return False
    return True


def pattern_matches_datasets(pattern: str, datasets: Iterable[str]) -> bool:
    return any(
        fnmatch.fnmatchcase(f"{signal}-{ds}-default", pattern)
        for ds in datasets for signal in _SIGNALS
    )


def eligibility(wf: LibraryWorkflow, datasets: Iterable[str], version: str | None) -> tuple[bool, str]:
    """Whether ``wf`` deploys for generators writing ``datasets``, with the reason."""
    datasets = set(datasets)
    if "observability" not in wf.solutions:
        return False, f"solutions {wf.solutions} has no observability"
    if not availability_ok(wf.meta.get("availability"), version):
        return False, f"availability {wf.meta.get('availability')} excludes {version}"
    if wf.install_form:
        return False, "needs install-form input: " + ", ".join(f.get("name", "?") for f in wf.install_form)
    required = wf.required_steps
    if not required:
        return False, "no required ES|QL step reads integration data"
    unmet = [p for s in required for p in _data_patterns(s.from_patterns)
             if not pattern_matches_datasets(p, datasets)]
    if unmet:
        return False, f"no enabled generator writes {sorted(set(unmet))}"
    return True, "every required step reads data an enabled generator writes"


def rule_wires_workflow(rule_esql: str, wf: LibraryWorkflow, datasets: Iterable[str]) -> bool:
    """Should this OOTB rule run ``wf`` when it fires?"""
    evidence = wf.evidence
    if evidence is None or "alert" not in wf.triggers or not wf.grouping:
        return False
    datasets = list(datasets)
    rule_from = _data_patterns(esql_from(rule_esql))
    shared = [ds for ds in datasets
              if any(fnmatch.fnmatchcase(f"{sig}-{ds}-default", p) for p in rule_from for sig in _SIGNALS)
              and any(pattern_matches_datasets(p, [ds]) for p in evidence.from_patterns)]
    if not shared:
        return False
    if not set(wf.grouping) & set(esql_group_columns(rule_esql)):
        return False
    return bool(esql_metric_names(rule_esql) & esql_metric_names(evidence.query))


def workflow_action(workflow_id: str) -> dict[str, Any]:
    """``.workflows`` system action: one run per new alert (``summaryMode: false``)."""
    return {
        "group": "query matched",
        "id": "system-connector-.workflows",
        "params": {
            "subAction": "run",
            "subActionParams": {"workflowId": workflow_id, "summaryMode": False},
        },
    }


def _strip_block(text: str, key: str) -> str:
    """Remove a top-level YAML block (``key:`` up to the next top-level key)."""
    return re.sub(rf"(?ms)^{re.escape(key)}:.*?(?=^[A-Za-z_][\w-]*:|\Z)", "", text, count=1)


def deployable_yaml(wf: LibraryWorkflow, input_values: dict[str, str] | None = None) -> str:
    """The template as the deployer posts it.

    Drops the library-only ``template-metadata`` block, declares ``enabled: true`` (an
    alert-triggered workflow created without it stays disabled), tags it
    ``ootb-workflow``, and fills ``input_values``: each becomes the ``default`` of the
    matching manual-trigger input, and replaces the literal fallback in
    ``inputs.<name> | default: '…'`` so alert-triggered runs (which carry no inputs) use
    it too. A fallback that reads the alert (``default: event…``) is left alone.
    """
    text = _strip_block(wf.text, "template-metadata").lstrip("\n")
    text = re.sub(r"(?m)^enabled:.*\n?", "", text)
    text = f"enabled: true\n{text}"
    if not re.search(r"(?m)^tags:", text):
        text = f"tags: [{OOTB_WORKFLOW_TAG}, {wf.slug}]\n{text}"
    for name, value in (input_values or {}).items():
        if name not in wf.manual_inputs or value in (None, ""):
            continue
        quoted = json.dumps(str(value))
        text = re.sub(_INPUT_FALLBACK.format(name=re.escape(name)),
                      lambda m: m.group(1) + "'" + str(value).replace("'", "") + "'", text)
        text = re.sub(
            rf"(?m)^(?P<indent>[ \t]*)- name: {re.escape(name)}[ \t]*\n(?P<body>(?:(?P=indent)[ \t]+(?!default:)\S.*\n)*)",
            lambda m: m.group(0) + f"{m.group('indent')}  default: {quoted}\n",
            text,
            count=1,
        )
    return text
