"""Shared helpers for provider-native (non-OTel) telemetry generators.

These generators write directly to Elasticsearch data streams via the
``_bulk?pipeline=_none`` API, so each document must be fully shaped before
it is sent.  This module provides the document scaffold, the Elasticsearch
``histogram`` type builder.

Business-load and counter utilities are imported from :mod:`db_common` so
they are not duplicated.

Public API
----------
- :func:`build_base_doc` — scaffold with ``@timestamp``, ``data_stream.*``,
  ``event.dataset``, ``agent.*`` and ``ecs.version``.
- :func:`emit_namespaces` — scenario namespace plus ``default`` for package dashboards.
- :func:`docs_for_namespace` — clone documents onto another data-stream namespace.
- :func:`send_dual_bulk` — bulk-index one batch to every emit namespace.
- :func:`build_histogram` — Elasticsearch ``histogram`` typed field value.
- :func:`exp_histogram_bounds` — exponential bucket boundaries (Vertex AI shape).
- :func:`business_load` — re-exported from :mod:`db_common`.
- :class:`CounterState` — re-exported from :mod:`db_common`.
- :func:`resolve_active_chaos` — re-exported from :mod:`db_common`.
"""

from __future__ import annotations

import datetime
import math
from typing import Any

# Re-export shared utilities so generator modules only need one import.
from generator_engine.db_common import (  # noqa: F401
    business_load,
    CounterState,
    resolve_active_chaos,
)

# Bump this when the document shape changes in a backward-incompatible way.
_AGENT_VERSION = "8.17.0"
_ECS_VERSION = "8.11.0"

# Package dashboards and OOTB alerts query ``*-default``.
OOTB_NAMESPACE = "default"
BULK_PARAMS = {"pipeline": "_none"}


def emit_namespaces(ns: str) -> list[str]:
    """Scenario namespace plus ``default`` when package assets need both."""
    if ns == OOTB_NAMESPACE:
        return [ns]
    return [ns, OOTB_NAMESPACE]


def docs_for_namespace(docs: list[dict[str, Any]], emit_ns: str) -> list[dict[str, Any]]:
    """Clone *docs* with ``data_stream.namespace`` set to *emit_ns*."""
    out: list[dict[str, Any]] = []
    for doc in docs:
        copied = dict(doc)
        data_stream = dict(doc.get("data_stream") or {})
        data_stream["namespace"] = emit_ns
        copied["data_stream"] = data_stream
        out.append(copied)
    return out


def send_dual_bulk(client: Any, dataset: str, ns: str, docs: list[dict[str, Any]]) -> None:
    """Bulk-index *docs* to every namespace returned by :func:`emit_namespaces`."""
    if not docs:
        return
    for emit_ns in emit_namespaces(ns):
        client.send_bulk(
            f"{dataset}-{emit_ns}",
            docs_for_namespace(docs, emit_ns),
            params=BULK_PARAMS,
        )


# ── Document scaffold ────────────────────────────────────────────────────────

def build_base_doc(
    *,
    timestamp: float,
    dataset: str,
    namespace: str,
    doc_type: str = "logs",
    agent_type: str = "filebeat",
    cloud: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the common envelope that every provider-native document carries.

    Parameters
    ----------
    timestamp:
        Unix epoch seconds (float), for example ``time.time()``.
    dataset:
        Elastic data-stream dataset name, for example ``"aws_bedrock.invocation"``.
    namespace:
        Scenario namespace, for example ``"banking"``.
    doc_type:
        ``"logs"`` or ``"metrics"``.
    agent_type:
        Elastic agent type string written to ``agent.type``.
    cloud:
        Optional dict with keys ``provider``, ``region`` and ``account_id``
        (or ``project_id`` for GCP).  Omit to leave cloud fields absent.

    Returns
    -------
    dict
        A mutable document dict.  The caller adds provider-specific fields.
    """
    ts_ms = int(timestamp * 1000)
    ts_iso = datetime.datetime.utcfromtimestamp(timestamp).strftime(
        "%Y-%m-%dT%H:%M:%S.") + f"{ts_ms % 1000:03d}Z"

    doc: dict[str, Any] = {
        "@timestamp": ts_iso,
        "ecs": {"version": _ECS_VERSION},
        "data_stream": {
            "type": doc_type,
            "dataset": dataset,
            "namespace": namespace,
        },
        "event": {
            "dataset": dataset,
        },
        "agent": {
            "type": agent_type,
            "version": _AGENT_VERSION,
        },
    }

    if cloud:
        doc["cloud"] = {}
        if "provider" in cloud:
            doc["cloud"]["provider"] = cloud["provider"]
        if "region" in cloud:
            doc["cloud"]["region"] = cloud["region"]
        # AWS uses account.id; GCP uses project.id; Azure uses account.id.
        if "account_id" in cloud:
            account: dict[str, str] = {"id": cloud["account_id"]}
            if cloud.get("account_name"):
                account["name"] = cloud["account_name"]
            doc["cloud"]["account"] = account
        if "project_id" in cloud:
            doc["cloud"]["project"] = {"id": cloud["project_id"]}

    return doc


# ── Elasticsearch histogram type ─────────────────────────────────────────────

def build_histogram(values: list[float], counts: list[int]) -> dict[str, Any]:
    """Build an Elasticsearch ``histogram`` typed field value.

    The Vertex AI integration stores latency distributions in this format.
    The ``values`` array must be in ascending order.  Each entry in ``counts``
    is the number of observations in the corresponding bucket.

    Parameters
    ----------
    values:
        Ascending bucket representative values (midpoints or boundaries).
    counts:
        Observation counts per bucket.  Must be the same length as ``values``.

    Returns
    -------
    dict
        ``{"values": [...], "counts": [...]}``
    """
    if len(values) != len(counts):
        raise ValueError(
            f"build_histogram: values length {len(values)} != counts length {len(counts)}"
        )
    if len(values) > 1:
        for i in range(1, len(values)):
            if values[i] <= values[i - 1]:
                raise ValueError(
                    f"build_histogram: values must be strictly ascending "
                    f"(values[{i - 1}]={values[i - 1]} >= values[{i}]={values[i]})"
                )
    return {"values": values, "counts": counts}


def exp_histogram_bounds(start: float = 1.0, ratio: float = 1.2, buckets: int = 48) -> list[float]:
    """Generate exponential bucket boundary values.

    Matches the distribution shape used by Vertex AI prediction latency metrics.
    Each boundary is ``start * ratio ** i`` for ``i`` in ``range(buckets)``.

    Parameters
    ----------
    start:
        First bucket boundary value.
    ratio:
        Multiplicative factor between successive boundaries.
    buckets:
        Total number of buckets.

    Returns
    -------
    list[float]
        Ascending bucket boundary values, rounded to three decimal places.
    """
    return [round(start * (ratio ** i), 3) for i in range(buckets)]


def make_histogram_counts(
    rng,
    bounds: list[float],
    total: int,
    peak_bucket: int | None = None,
) -> list[int]:
    """Distribute ``total`` observations across histogram buckets.

    Concentrates most counts near ``peak_bucket`` (or the middle of the range)
    using a rough normal-ish distribution, then fills the tail sparsely.

    Parameters
    ----------
    rng:
        A :class:`random.Random` instance for reproducibility.
    bounds:
        Bucket boundaries from :func:`exp_histogram_bounds`.
    total:
        Total number of observations to distribute.
    peak_bucket:
        Index of the bucket that should receive the most counts.  Defaults to
        one-third of the way along the range (the skew typical of latency data).

    Returns
    -------
    list[int]
        One count per bucket.  Counts sum to ``total``.
    """
    n = len(bounds)
    if peak_bucket is None:
        peak_bucket = n // 3
    # Generate raw weights: gaussian-ish falloff from the peak.
    weights = [math.exp(-0.5 * ((i - peak_bucket) / (n / 6)) ** 2) for i in range(n)]
    total_w = sum(weights)
    counts = [int(total * w / total_w) for w in weights]
    # Distribute the rounding remainder.
    remainder = total - sum(counts)
    for _ in range(remainder):
        idx = rng.randint(0, n - 1)
        counts[idx] += 1
    return counts
