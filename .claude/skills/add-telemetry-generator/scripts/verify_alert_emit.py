#!/usr/bin/env python3
"""Static check: generator sources mention alert-required fields.

Loads ``alert_required_fields.json`` (from extract_alert_esql_fields.py) and
scans ``generators/<id>/**/*.py`` for string literals / identifiers. This is a
cheap pre-deploy gate — not a substitute for live Kibana rule execution.

Usage::

    python3 verify_alert_emit.py \\
        --inventory /tmp/pkg/alert_required_fields.json \\
        --generator-dir generators/aws_lambda_otel

Exit codes:
  0 — all required tokens found (or inventory empty)
  1 — missing emit coverage
  2 — usage / I/O error
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def _builder_source(generator_dir: Path) -> str:
    """Source of ``build_signal_resource``, which sets service.instance.id and the other
    resource attributes for every generator that calls it."""
    engine = generator_dir.resolve().parent.parent / "generator_engine" / "db_common.py"
    try:
        text = engine.read_text(encoding="utf-8")
    except OSError:
        return ""
    start = text.find("def build_signal_resource(")
    if start == -1:
        return ""
    end = text.find("\ndef ", start + 1)
    return text[start:end if end != -1 else len(text)]


def _read_sources(generator_dir: Path) -> str:
    chunks: list[str] = []
    for path in sorted(generator_dir.rglob("*.py")):
        try:
            chunks.append(path.read_text(encoding="utf-8"))
        except OSError as exc:
            print(f"warn: skip {path}: {exc}", file=sys.stderr)
    source = "\n".join(chunks)
    if "build_signal_resource" in source:
        source += "\n" + _builder_source(generator_dir)
    return source


def _token_present(source: str, field: str) -> bool:
    """Return True if the generator likely emits ``field``.

    Matches:
      - exact string literals (\"field\" / 'field')
      - trailing segment for dotted attrs (evidence.role → \"role\" alone is weak;
        prefer full dotted form)
      - CloudWatch path segment (DeadLetterErrors in amazonaws.com/AWS/Lambda/...)
      - attributes.X → look for \"X\" as attr key in dict literals
    """
    if not field:
        return True
    # Full token as string literal.
    lit = re.compile(rf"""['"]{re.escape(field)}['"]""")
    if lit.search(source):
        return True
    # Strip metrics. / attributes. / resource.attributes. prefixes.
    bare = field
    for prefix in ("metrics.", "attributes.", "resource.attributes."):
        if bare.startswith(prefix):
            bare = bare[len(prefix) :]
            break
    if bare != field:
        lit_bare = re.compile(rf"""['"]{re.escape(bare)}['"]""")
        if lit_bare.search(source):
            return True
    # CloudWatch MetricName often appears as the last path segment.
    if "/" in field:
        seg = field.rsplit("/", 1)[-1]
        if re.search(rf"""['"]{re.escape(seg)}['"]""", source):
            return True
    # Final dotted segment for long azure.* paths (role, type, is_synthetic).
    if "." in bare:
        leaf = bare.rsplit(".", 1)[-1]
        # Require a nearby parent segment to reduce false positives on short leaves.
        parent = bare.rsplit(".", 1)[0].rsplit(".", 1)[-1]
        if len(leaf) >= 3 and parent in source and re.search(
            rf"""['"]{re.escape(leaf)}['"]""", source
        ):
            return True
        if re.search(rf"""['"]{re.escape(bare)}['"]""", source):
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inventory", type=Path, required=True)
    ap.add_argument("--generator-dir", type=Path, required=True)
    ap.add_argument(
        "--strict-coalesce",
        action="store_true",
        default=True,
        help="Require every coalesce_operands[] token (default: true)",
    )
    args = ap.parse_args(argv)

    if not args.inventory.is_file():
        print(f"error: inventory not found: {args.inventory}", file=sys.stderr)
        return 2
    if not args.generator_dir.is_dir():
        print(f"error: generator dir not found: {args.generator_dir}", file=sys.stderr)
        return 2

    inv = json.loads(args.inventory.read_text(encoding="utf-8"))
    source = _read_sources(args.generator_dir)

    required: list[str] = []
    required.extend(inv.get("counter_required") or [])
    required.extend(inv.get("coalesce_operands") or [])
    # Always check CloudWatch dimension columns when flagged.
    if inv.get("requires_cloudwatch_namespace_metricname"):
        required.extend(["Namespace", "MetricName"])
    # High-signal fields: attributes.*, azure.*, Namespace filters, etc.
    for f in inv.get("fields") or []:
        if f in required:
            continue
        if f.startswith(("attributes.", "azure.", "container.", "k8s.", "server.")):
            required.append(f)
        elif f in ("Namespace", "MetricName", "log.level", "service.name"):
            required.append(f)
        elif f.startswith("envoy.http.admin."):
            required.append(f)
        elif "traefik_router" in f or f.endswith("_requests_total"):
            # Router-level series often missing when only entrypoint/service emitted.
            if "router" in f:
                required.append(f)

    # De-dupe preserving order.
    seen: set[str] = set()
    ordered: list[str] = []
    for f in required:
        if f not in seen:
            seen.add(f)
            ordered.append(f)

    missing = [f for f in ordered if not _token_present(source, f)]

    # from_streams dataset alignment hint (non-fatal unless --fail-datasets).
    streams = inv.get("from_streams") or []
    print(f"alerts={inv.get('alert_count', 0)} required_tokens={len(ordered)} "
          f"from_streams={streams}")

    if inv.get("requires_cloudwatch_namespace_metricname"):
        cw_ok = _token_present(source, "Namespace") and _token_present(source, "MetricName")
        # Also accept build_cloudwatch_metric which now sets them centrally.
        if not cw_ok and "build_cloudwatch_metric" in source:
            cw_ok = True
            print("ok: CloudWatch Namespace/MetricName via build_cloudwatch_metric()")
            missing = [m for m in missing if m not in ("Namespace", "MetricName")]
        elif cw_ok:
            print("ok: CloudWatch Namespace/MetricName present in generator sources")
        else:
            if "Namespace" not in missing:
                missing.append("Namespace")
            if "MetricName" not in missing:
                missing.append("MetricName")

    if not ordered:
        print("ok: no alert field tokens to check (empty inventory)")
        return 0

    if missing:
        print("MISSING alert emit coverage:")
        for m in missing:
            print(f"  - {m}")
        print(
            "\nFix generators to emit these fields/attrs (do not edit OOTB alerts). "
            "See ALERTS.md in this skill."
        )
        return 1

    print(f"ok: all {len(ordered)} required alert tokens found in {args.generator_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
