#!/usr/bin/env python3
"""Audit generators against the Elastic integration packages they install.

For every generator that cites a package (``SOURCE_INTEGRATION``), download the cited
package — the version the deployer installs — from the public Elastic Package Registry
(in memory) and report (``--latest`` audits the newest release instead, to preview an
upgrade):

  * stale citation          a newer release than the cited (installed) version exists
  * alerts without a key    package alert templates no ``SPEC.impact_keys`` doc names
  * stale alert keys        impact-key docs that name an alert the package no longer ships
  * missing fields          fields/metrics the package's dashboards and alert queries aggregate
                            that the generator source never mentions
  * missing metric+stat     ``MetricName == "X"`` with ``stat == "Y"`` where the generator
                            source has no ``"X"`` near ``"Y"``
  * alert-emit gate         the repo's own ``extract_alert_esql_fields.py`` +
                            ``verify_alert_emit.py`` (add-telemetry-generator skill) must pass
  * template-emit gate      ``extract_package_templates.py`` + ``verify_template_emit.py``:
                            fields and selector values the package's SLO templates and ML
                            modules read (the runtime proof is tests/test_slo_template_emit.py
                            and tests/test_ml_module_emit.py)

The checks are textual heuristics over the generator source, so a hit means "look here",
not "definitely broken". Run it after bumping a ``SOURCE_INTEGRATION``. Needs network.

Usage::

    python3 scripts/audit_package_coverage.py                    # all generators
    python3 scripts/audit_package_coverage.py aws_ecs_otel       # one or more
    python3 scripts/audit_package_coverage.py --json             # machine-readable
    python3 scripts/audit_package_coverage.py --ignore-stale     # skip the citation check

Exit status is 1 when any gap is reported.
"""
from __future__ import annotations

import argparse
import io
import json
import re
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

EPR = "https://epr.elastic.co"
# A field name may quote any segment in backticks: `Duration (ms)`, supabase.realtime.`limit`.
FIELD = r"((?:[A-Za-z_][\w.\-/:@]*|`[^`]+`)(?:\.?`[^`]+`|[\w.\-/:@]+)*)"
AGG = re.compile(
    r"(?:SUM|AVG|MAX|MIN|LAST|FIRST|COUNT|COUNT_DISTINCT|RATE|INCREASE|PERCENTILE|MEDIAN|"
    r"VALUES|TOP|IRATE|DELTA|[A-Z]+_OVER_TIME)\(\s*" + FIELD
)
BACKTICKED = re.compile(r"`(metrics\.[^`]+)`")
ALIAS = re.compile(r"(?:\b([A-Za-z_]\w*)|`([^`]+)`)\s*=(?!=)")
METRIC_NAME = re.compile(r"""MetricName\s*(?:==|IN)\s*\(?\s*"([^"]+)"[^|]*?stat\s*==\s*"([^"]+)"|"""
                         r"""stat\s*==\s*"([^"]+)"[^|]*?MetricName\s*==\s*"([^"]+)\"""", re.S)
SOURCE_CITATION = re.compile(r"""SOURCE_INTEGRATION\s*=\s*[\(\"']([^\n]*)""")
PACKAGE_VERSION = re.compile(r"(?:packages/)?([a-z0-9_]+)@(\d[\w.\-]*)")
ALERT_DOC = re.compile(r'Drive alert "([^"]+)"')
SKILL_SCRIPTS = ROOT / ".claude" / "skills" / "add-telemetry-generator" / "scripts"


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "audit-package-coverage"})
    return urllib.request.urlopen(request, timeout=60).read()


def latest_version(package: str) -> str:
    return json.loads(_get(f"{EPR}/search?package={package}"))[0]["version"]


def package_zip(package: str, version: str) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(_get(f"{EPR}/epr/{package}/{package}-{version}.zip")))


def _strings(node, out: list[str]) -> None:
    if isinstance(node, dict):
        for value in node.values():
            _strings(value, out)
    elif isinstance(node, list):
        for value in node:
            _strings(value, out)
    elif isinstance(node, str):
        out.append(node)


def read_package(zf: zipfile.ZipFile) -> tuple[list[str], list[str]]:
    """Return (alert template names, ES|QL-ish query strings from dashboards and alerts)."""
    alerts: list[str] = []
    queries: list[str] = []
    for name in zf.namelist():
        if "/kibana/" not in name or not name.endswith(".json"):
            continue
        doc = json.loads(zf.read(name))
        if "/alerting_rule_template/" in name:
            alerts.append(doc.get("attributes", {}).get("name") or name)
        texts: list[str] = []
        _strings(doc, texts)
        queries.extend(t for t in texts if "FROM" in t.upper() or "metrics." in t)
    return alerts, queries


def referenced_fields(queries: list[str]) -> set[str]:
    fields: set[str] = set()
    for query in queries:
        aliases = {plain or quoted for plain, quoted in ALIAS.findall(query)}
        # Columns a GROK / DISSECT pattern extracts (``%{GREEDYDATA:Message}``, ``%{status}``).
        aliases |= set(re.findall(r"%\{(?:[A-Z0-9_]+:)?([A-Za-z_][\w.]*)\}", query))
        for raw in AGG.findall(query) + BACKTICKED.findall(query):
            if raw.startswith("`") and raw.endswith("`") and raw[1:-1] in aliases:
                continue
            raw = raw.replace("`", "")
            name = raw[len("metrics."):] if raw.startswith("metrics.") else raw
            if "/AWS/" in name:  # CloudWatch path; generators spell just the metric name
                name = name.rsplit("/", 1)[-1]
            if (len(name) <= 2 or name in aliases or name.startswith("?")
                    or name.isupper() or name == "@timestamp"
                    or re.fullmatch(r"[a-z]+_over_time", name)):
                continue
            fields.add(name)
    return fields


def metric_stat_pairs(queries: list[str]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for query in queries:
        for m in METRIC_NAME.finditer(query):
            metric, stat = (m.group(1), m.group(2)) if m.group(1) else (m.group(4), m.group(3))
            pairs.add((metric, stat))
    return pairs


def stat_near_metric(source: str, metric: str, stat: str, window: int) -> bool:
    """True when ``stat`` appears within ``window`` characters after any ``metric`` mention."""
    start = source.find(f'"{metric}"')
    while start != -1:
        if f'"{stat}"' in source[start:start + window]:
            return True
        start = source.find(f'"{metric}"', start + 1)
    return False


def alert_emit_gate(zf: zipfile.ZipFile, generator_id: str) -> list[str]:
    """Run the skill's extract + verify scripts on the package's alert templates.

    Returns the lines of the verify output that report missing coverage; empty when it
    passes (or the package ships no alert templates).
    """
    extract = SKILL_SCRIPTS / "extract_alert_esql_fields.py"
    verify = SKILL_SCRIPTS / "verify_alert_emit.py"
    templates = [n for n in zf.namelist()
                 if "/alerting_rule_template/" in n and n.endswith(".json")]
    if not (templates and extract.is_file() and verify.is_file()):
        return []
    with tempfile.TemporaryDirectory() as tmp:
        tdir = Path(tmp) / "templates"
        tdir.mkdir()
        for name in templates:
            (tdir / Path(name).name).write_bytes(zf.read(name))
        inventory = Path(tmp) / "alert_required_fields.json"
        subprocess.run([sys.executable, "-I", str(extract), "--templates-dir", str(tdir),
                        "--out", str(inventory)], capture_output=True, text=True, check=True)
        done = subprocess.run(
            [sys.executable, "-I", str(verify), "--inventory", str(inventory),
             "--generator-dir", str(ROOT / "generators" / generator_id)],
            capture_output=True, text=True)
    if done.returncode == 0:
        return []
    return [ln.strip() for ln in (done.stdout + done.stderr).splitlines()
            if ln.strip() and not ln.startswith("ok:")]


def template_emit_gate(zf: zipfile.ZipFile, generator_id: str) -> list[str]:
    """Run the skill's SLO-template / ML-module extract + verify scripts on the package.

    Returns the lines of the verify output that report missing coverage; empty when it
    passes (or the package ships neither asset type).
    """
    extract = SKILL_SCRIPTS / "extract_package_templates.py"
    verify = SKILL_SCRIPTS / "verify_template_emit.py"
    assets = [n for n in zf.namelist()
              if ("/slo_template/" in n or "/ml_module/" in n) and n.endswith(".json")]
    if not (assets and extract.is_file() and verify.is_file()):
        return []
    with tempfile.TemporaryDirectory() as tmp:
        pkg_dir = Path(tmp) / "package"
        for name in assets + [n for n in zf.namelist() if n.endswith("/manifest.yml") and n.count("/") == 1]:
            target = pkg_dir / name.split("/", 1)[1]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(zf.read(name))
        out = Path(tmp) / "inventory"
        subprocess.run([sys.executable, "-I", str(extract), "--package-dir", str(pkg_dir),
                        "--out-dir", str(out)], capture_output=True, text=True, check=True)
        done = subprocess.run(
            [sys.executable, "-I", str(verify), "--inventory-dir", str(out),
             "--generator-dir", str(ROOT / "generators" / generator_id)],
            capture_output=True, text=True)
    if done.returncode == 0:
        return []
    return [ln.strip() for ln in (done.stdout + done.stderr).splitlines()
            if ln.strip().startswith("- ")]


def mentions(source: str, field: str) -> bool:
    """True when ``field`` appears as a whole dotted name, not inside a longer one.

    ``container.cpu.usage`` must not count as present because the source emits
    ``k8s.container.cpu.usage``; a plain substring test hid exactly that gap.

    Generators write attribute keys without the ``attributes.`` prefix that queries
    use, and ``log.level`` is an alias of the record's severity in ``logs-*.otel``.
    """
    if field == "log.level":
        return "severity" in source.lower()
    if re.search(rf"(?<![\w.]){re.escape(field)}(?![\w])", source):
        return True
    # An unprefixed attribute key must appear as a quoted key, so a short name like
    # ``topic`` is not satisfied by prose or an unrelated identifier.
    key = field.removeprefix("attributes.")
    return key != field and re.search(rf"""["']{re.escape(key)}["']""", source) is not None


def builder_source() -> str:
    text = (ROOT / "generator_engine" / "db_common.py").read_text()
    start = text.find("def build_signal_resource(")
    end = text.find("\ndef ", start + 1)
    return text[start:end if end != -1 else len(text)] if start != -1 else ""


def audit(generator_id: str, *, window: int, check_stale: bool, against_latest: bool = False) -> dict:
    source_path = ROOT / "generators" / generator_id / "generator.py"
    source = source_path.read_text()
    if "build_signal_resource" in source:  # sets service.instance.id etc. for every caller
        source += "\n" + builder_source()
    init = (ROOT / "generators" / generator_id / "__init__.py").read_text()
    citation = SOURCE_CITATION.search(source)
    found = PACKAGE_VERSION.search(citation.group(1)) if citation else None
    if not found:
        return {"generator": generator_id, "skipped": "no package citation"}
    package, cited = found.groups()
    latest = latest_version(package)
    archive = package_zip(package, latest if against_latest else cited)
    alerts, queries = read_package(archive)

    result = {
        "generator": generator_id,
        "package": package,
        "cited": cited,
        "latest": latest,
        "stale_citation": check_stale and cited != latest,
        "package_alerts": len(alerts),
        "alerts_without_key": sorted(a for a in alerts if a not in init),
        "stale_alert_keys": sorted({a for a in ALERT_DOC.findall(init)} - set(alerts)) if alerts else [],
        "missing_fields": sorted(
            f for f in referenced_fields(queries)
            if not mentions(source, f) and not mentions(source, f.removeprefix("resource.attributes."))
        ),
        "missing_metric_stat": sorted(
            f"{metric} / {stat}" for metric, stat in metric_stat_pairs(queries)
            if metric in source and not stat_near_metric(source, metric, stat, window)
        ),
        "alert_emit_gate": alert_emit_gate(archive, generator_id),
        "template_emit_gate": template_emit_gate(archive, generator_id),
    }
    result["gaps"] = bool(
        result["stale_citation"] or result["alerts_without_key"] or result["stale_alert_keys"]
        or result["missing_fields"] or result["missing_metric_stat"]
        or result["alert_emit_gate"] or result["template_emit_gate"]
    )
    return result


def generator_ids() -> list[str]:
    return sorted(p.parent.name for p in (ROOT / "generators").glob("*/generator.py"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("generators", nargs="*", help="generator ids (default: all)")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument("--ignore-stale", action="store_true", help="do not flag old citations")
    parser.add_argument("--latest", action="store_true",
                        help="audit against the newest release instead of the cited one (upgrade preview)")
    parser.add_argument("--window", type=int, default=400,
                        help="characters after a metric name to look for its stat (default 400)")
    args = parser.parse_args()

    results = []
    for gid in args.generators or generator_ids():
        try:
            results.append(audit(gid, window=args.window, check_stale=not args.ignore_stale,
                                 against_latest=args.latest))
        except Exception as exc:  # network or parse failure for one package must not hide the rest
            results.append({"generator": gid, "error": f"{type(exc).__name__}: {exc}", "gaps": True})

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            if r.get("skipped") or not r.get("gaps"):
                continue
            print(f"\n{r['generator']}  [{r.get('cited', '?')} -> {r.get('latest', '?')}]")
            if r.get("error"):
                print(f"   ERROR {r['error']}")
            if r.get("stale_citation"):
                print(f"   stale citation: cites {r['cited']}, latest is {r['latest']}")
            for key, label in (("alerts_without_key", "alert has no impact-key doc"),
                               ("stale_alert_keys", "impact-key doc names a removed alert"),
                               ("missing_fields", "field not in generator"),
                               ("missing_metric_stat", "metric not emitted with this stat"),
                               ("alert_emit_gate", "alert-emit gate"),
                               ("template_emit_gate", "SLO/ML template-emit gate")):
                for item in r.get(key, []):
                    print(f"   {label}: {item}")
        checked = [r for r in results if not r.get("skipped")]
        clean = [r for r in checked if not r["gaps"]]
        print(f"\n{len(clean)}/{len(checked)} generators clean "
              f"({len(results) - len(checked)} without a package citation)")
    return 1 if any(r.get("gaps") for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
