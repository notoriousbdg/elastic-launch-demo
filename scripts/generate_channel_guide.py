#!/usr/bin/env python3
"""
generate_channel_guide.py — Elastic Chaos Channel Demo Guide Generator

Reads a scenario's channel + service YAMLs and emits a self-contained HTML
presenter (dark slide deck with cascade-flow diagrams) into
scenarios/<id>/demo-guide/chaos-channels.html

Usage:
    python3 scripts/generate_channel_guide.py relief
    python3 scripts/generate_channel_guide.py relief --out /tmp/preview.html
    python3 scripts/generate_channel_guide.py --all
    python3 scripts/generate_channel_guide.py --all --out-dir /tmp/guides
"""

import argparse
import html as html_mod
import json
import random
import sys
from pathlib import Path

import yaml

# ── repo root on sys.path so scenario_engine is importable ──────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scenario_engine import get_scenario, list_scenarios  # noqa: E402

# ── cloud constants ──────────────────────────────────────────────────────────
CLOUD_COLORS = {"aws": "#FF9900", "gcp": "#4285F4", "azure": "#0078D4"}
CLOUD_LABELS = {"aws": "AWS",     "gcp": "GCP",     "azure": "Azure"}


# ── data loaders ────────────────────────────────────────────────────────────

def load_services(scenario_dir: Path) -> dict:
    """Return {svc_name: {provider, region, subsystem, language, sort_order}}."""
    result = {}
    svc_dir = scenario_dir / "services"
    if not svc_dir.exists():
        return result
    for p in sorted(svc_dir.glob("*.yaml")):
        data = yaml.safe_load(p.read_text()) or {}
        name = data.get("service", p.stem)
        result[name] = {
            "service":    name,
            "provider":   data.get("cloud_provider", "aws"),
            "region":     data.get("cloud_region", ""),
            "zone":       data.get("cloud_availability_zone", ""),
            "subsystem":  data.get("subsystem", ""),
            "language":   data.get("language", ""),
            "sort_order": data.get("sort_order", 99),
        }
    return result


def load_channels(scenario_dir: Path) -> dict:
    """Return {ch_num: full_yaml_dict} — includes correlation_attr, fault_params, rca_clues."""
    result = {}
    ch_dir = scenario_dir / "channels"
    if not ch_dir.exists():
        return result
    for p in sorted(ch_dir.glob("*.yaml")):
        try:
            num = int(p.stem.split("-", 1)[0])
        except ValueError:
            continue
        data = yaml.safe_load(p.read_text()) or {}
        data["_num"] = num
        result[num] = data
    return result


def resolve_examples(fault_params: dict) -> dict:
    """Return one concrete resolved value per fault_param for display (seeded)."""
    rng = random.Random(42)
    out = {}
    for k, spec in (fault_params or {}).items():
        if not isinstance(spec, dict):
            out[k] = spec
            continue
        if "randint" in spec:
            lo, hi = spec["randint"]
            out[k] = rng.randint(lo, hi)
        elif "uniform" in spec:
            lo, hi = spec["uniform"]
            out[k] = round(rng.uniform(lo, hi), 2)
        elif "choice" in spec:
            opts = spec["choice"]
            out[k] = rng.choice(opts) if opts else ""
        elif "format" in spec:
            out[k] = spec["format"]
        else:
            out[k] = str(spec)
    return out


# ── slide builder ────────────────────────────────────────────────────────────

def build_slides(scenario, channels: dict, services: dict) -> list:
    """Build the ordered slide array from scenario + channel data."""
    sname    = scenario.scenario_name
    icon     = getattr(scenario, "scenario_icon", "⚡")
    nominal  = getattr(scenario, "nominal_label", "NOMINAL")
    desc     = getattr(scenario, "scenario_description", "")

    # group services by cloud provider (maintaining sort_order)
    cloud_groups: dict[str, list] = {}
    for svc in sorted(services.values(), key=lambda s: s["sort_order"]):
        cloud_groups.setdefault(svc["provider"], []).append(svc)

    cloud_list = [
        {"provider": prov, "region": svcs[0]["region"] if svcs else "",
         "services": svcs}
        for prov in ["aws", "gcp", "azure"]
        if (svcs := cloud_groups.get(prov, []))
    ]

    slides = []

    # ── 0: Title ──────────────────────────────────────────────────────────────
    slides.append({
        "type":        "title",
        "icon":        icon,
        "name":        sname,
        "subtitle":    "20 Chaos Channels — Demo Guide",
        "description": desc,
        "meta": {
            "Channels":  "20 Fault Channels",
            "Services":  f"{len(services)} Microservices",
            "Clouds":    "AWS · GCP · Azure",
        },
    })

    # ── 1: TOC ─────────────────────────────────────────────────────────────────
    slides.append({
        "type": "toc",
        "summary": [
            {
                "num":       num,
                "name":      channels[num].get("name", ""),
                "subsystem": channels[num].get("subsystem", ""),
                "error_type":channels[num].get("error_type", ""),
                "is_fault":  num <= 15,
            }
            for num in sorted(channels)
        ],
    })

    # ── 2: Platform / service map ─────────────────────────────────────────────
    slides.append({
        "type":   "platform",
        "icon":   icon,
        "name":   sname,
        "clouds": cloud_list,
    })

    def make_channel_slide(num: int, is_fault: bool) -> dict:
        ch = channels[num]
        all_svcs = (
            set(ch.get("affected_services") or []) |
            set(ch.get("cascade_services")  or [])
        )
        svc_cloud = {
            s: {
                "provider": services.get(s, {}).get("provider", "aws"),
                "region":   services.get(s, {}).get("region", ""),
            }
            for s in all_svcs
        }
        fp = ch.get("fault_params") or {}
        rca_keys = list((ch.get("rca_clues") or {}).keys())
        return {
            "type":               "channel",
            "num":                num,
            "name":               ch.get("name", ""),
            "subsystem":          ch.get("subsystem", ""),
            "error_type":         ch.get("error_type", ""),
            "sensor_type":        ch.get("sensor_type", ""),
            "affected_services":  ch.get("affected_services") or [],
            "cascade_services":   ch.get("cascade_services")  or [],
            "description":        (ch.get("description") or "").strip(),
            "investigation_notes":(ch.get("investigation_notes") or "").strip(),
            "remediation_action": ch.get("remediation_action", "restart_service"),
            "error_message":      (ch.get("error_message") or "").strip(),
            "correlation_attr":   ch.get("correlation_attr") or {},
            "rca_clues":          rca_keys,
            "example_values":     resolve_examples(fp),
            "svc_cloud":          svc_cloud,
            "is_fault":           is_fault,
            "nominal_label":      nominal,
        }

    # ── 3: Section — Faults ───────────────────────────────────────────────────
    slides.append({
        "type":  "section",
        "part":  "Part 1",
        "title": "Fault Channels (1–15)",
        "desc":  "True faults — AI agent investigation & approval required",
        "color": "#f85149",
    })

    # ── 4-18: Channels 1-15 ───────────────────────────────────────────────────
    for num in range(1, 16):
        if num in channels:
            slides.append(make_channel_slide(num, True))

    # ── 19: Section — Benign ──────────────────────────────────────────────────
    slides.append({
        "type":  "section",
        "part":  "Part 2",
        "title": "Benign & Self-Healing (16–20)",
        "desc":  "System events that fire but self-resolve — signal vs. noise",
        "color": "#3fb950",
    })

    # ── 20-24: Channels 16-20 ─────────────────────────────────────────────────
    for num in range(16, 21):
        if num in channels:
            slides.append(make_channel_slide(num, False))

    # ── 25: Cheatsheet ────────────────────────────────────────────────────────
    slides.append({
        "type": "cheatsheet",
        "channels": [
            {
                "num":       num,
                "name":      channels[num].get("name", ""),
                "subsystem": channels[num].get("subsystem", ""),
                "error_type":channels[num].get("error_type", ""),
                "affected":  channels[num].get("affected_services") or [],
                "cascade":   channels[num].get("cascade_services")  or [],
                "is_fault":  num <= 15,
            }
            for num in sorted(channels)
        ],
    })

    return slides


# ── HTML renderer ────────────────────────────────────────────────────────────

# The template uses __PLACEHOLDER__ tokens so we avoid f-string vs JS-brace conflicts.
_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__TITLE__ — Chaos Channel Guide</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:       #0d1117;
    --bg2:      #161b22;
    --border:   #30363d;
    --text:     #c9d1d9;
    --muted:    #8b949e;
    --white:    #ffffff;
    --yellow:   #FEC514;
    --blue:     #58a6ff;
    --blue2:    #1f6feb;
    --critical: #f85149;
    --warning:  #d29922;
    --nominal:  #3fb950;
    --aws:      #FF9900;
    --gcp:      #4285F4;
    --azure:    #0078D4;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ── TOPBAR ── */
  .topbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 0 28px;
    height: 52px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-shrink: 0;
    position: relative;
  }
  .topbar-brand { font-size: 13px; font-weight: 700; color: var(--blue); letter-spacing: 1px; }
  .topbar-brand span { color: var(--yellow); }
  .topbar-title { font-size: 14px; font-weight: 600; color: var(--white); }
  .topbar-counter { font-size: 13px; color: var(--muted); }
  .progress-bar {
    position: absolute; bottom: 0; left: 0;
    height: 3px; background: var(--blue2); transition: width 0.2s ease;
  }

  /* ── STAGE ── */
  .stage {
    flex: 1; overflow-y: auto; padding: 28px 52px;
    display: flex; flex-direction: column; justify-content: flex-start;
  }
  .stage.fade { opacity: 0; transition: opacity 0.18s; }

  /* ── BOTTOMBAR ── */
  .bottombar {
    background: var(--bg2); border-top: 1px solid var(--border);
    display: flex; flex-direction: column; gap: 6px;
    padding: 8px 28px 10px; flex-shrink: 0;
  }
  .dots-nav-row { display: flex; align-items: center; justify-content: center; }
  .dots-row { display: flex; justify-content: center; gap: 4px; flex-wrap: wrap; }
  .dot-btn {
    width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid var(--border); background: transparent;
    cursor: pointer; padding: 0; transition: border-color .15s, background .15s;
  }
  .dot-btn:hover { border-color: var(--blue); }
  .dot-btn.active { background: var(--blue2); border-color: var(--blue2); }
  .dot-btn[data-kind="section"]   { border-color: var(--yellow); }
  .dot-btn[data-kind="title"],
  .dot-btn[data-kind="toc"],
  .dot-btn[data-kind="platform"]  { border-color: var(--yellow); }
  .dot-btn[data-kind="fault"]     { border-color: var(--critical); }
  .dot-btn[data-kind="benign"]    { border-color: var(--nominal); }
  .dot-btn[data-kind="cheatsheet"]{ border-color: var(--blue); }
  .toc-jump-btn {
    margin-left: 40px; background: transparent; color: var(--blue);
    border: 1px solid var(--border); border-radius: 6px;
    padding: 4px 14px; font-size: 12px; font-weight: 600;
    cursor: pointer; letter-spacing: .5px; white-space: nowrap;
    transition: background .15s, border-color .15s;
  }
  .toc-jump-btn:hover { background: rgba(31,111,235,.15); border-color: var(--blue2); }
  .toc-jump-btn.hidden { visibility: hidden; pointer-events: none; }
  .nav-row { display: flex; align-items: center; justify-content: space-between; }
  .nav-btn {
    background: #21262d; color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 6px 20px; font-size: 13px; font-weight: 600;
    cursor: pointer; transition: background .15s, border-color .15s, color .15s;
  }
  .nav-btn:hover { background: var(--blue2); border-color: var(--blue2); color: var(--white); }
  .nav-btn:disabled { opacity: .3; cursor: default; }
  .nav-btn:disabled:hover { background: #21262d; border-color: var(--border); color: var(--text); }
  .key-hint { font-size: 12px; color: var(--muted); }

  /* ── TITLE SLIDE ── */
  .title-slide { text-align: center; max-width: 800px; margin: 32px auto 0; }
  .title-icon { font-size: 60px; margin-bottom: 14px; }
  .title-eyebrow {
    display: inline-block; background: var(--yellow); color: var(--bg);
    font-size: 11px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase;
    padding: 4px 14px; border-radius: 3px; margin-bottom: 20px;
  }
  .title-main { font-size: 38px; font-weight: 700; color: var(--white); line-height: 1.2; margin-bottom: 10px; }
  .title-sub  { font-size: 18px; color: var(--muted); margin-bottom: 32px; }
  .title-meta { display: flex; gap: 16px; justify-content: center; flex-wrap: wrap; }
  .meta-card {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
    padding: 14px 20px; min-width: 140px;
  }
  .meta-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .meta-value { font-size: 15px; font-weight: 700; color: var(--blue); }

  /* ── TOC SLIDE ── */
  .toc-slide { max-width: 1000px; margin: 0 auto; width: 100%; }
  .slide-eyebrow { font-size: 11px; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; color: var(--yellow); margin-bottom: 14px; }
  .toc-filter {
    width: 100%; background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; padding: 8px 14px; color: var(--text); font-size: 13px;
    margin-bottom: 14px; outline: none;
  }
  .toc-filter:focus { border-color: var(--blue2); }
  .toc-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(195px, 1fr));
    gap: 8px; overflow-y: auto; max-height: calc(100vh - 240px); padding-right: 4px;
  }
  .toc-tile {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; padding: 10px 14px; cursor: pointer;
    transition: border-color .15s, background .15s;
  }
  .toc-tile:hover { border-color: var(--blue); background: #1c2128; }
  .toc-tile[data-fault="1"] { border-left: 3px solid var(--critical); }
  .toc-tile[data-fault="0"] { border-left: 3px solid var(--nominal); }
  .toc-num  { font-size: 10px; color: var(--muted); margin-bottom: 3px; }
  .toc-err  { font-size: 10px; font-weight: 700; color: var(--yellow); letter-spacing: .8px; margin-bottom: 3px; }
  .toc-name { font-size: 12px; color: var(--text); line-height: 1.4; }
  .toc-sub  { font-size: 10px; color: var(--muted); margin-top: 4px; text-transform: uppercase; letter-spacing: .5px; }
  .toc-hidden { display: none !important; }

  /* ── PLATFORM SLIDE ── */
  .platform-slide { max-width: 1000px; margin: 0 auto; width: 100%; }
  .cloud-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
  .cloud-col {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px;
  }
  .cloud-header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 14px; padding-bottom: 10px; border-bottom: 1px solid var(--border);
  }
  .cloud-badge {
    font-size: 11px; font-weight: 700; color: var(--bg);
    padding: 2px 8px; border-radius: 3px; text-transform: uppercase;
  }
  .cloud-region { font-size: 11px; color: var(--muted); }
  .svc-card {
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 10px 12px; margin-bottom: 8px;
  }
  .svc-card:last-child { margin-bottom: 0; }
  .svc-name { font-size: 13px; font-weight: 600; color: var(--white); margin-bottom: 5px; }
  .badges { display: flex; gap: 5px; flex-wrap: wrap; }
  .badge {
    font-size: 10px; font-weight: 600; padding: 2px 6px; border-radius: 3px;
    text-transform: uppercase; letter-spacing: .4px;
  }
  .badge-sub  { background: rgba(88,166,255,.14); color: var(--blue); }
  .badge-lang { background: rgba(254,197,20,.12);  color: var(--yellow); }

  /* ── SECTION BREAK ── */
  .section-break { text-align: center; max-width: 700px; margin: 56px auto 0; }
  .section-part  { font-size: 80px; font-weight: 700; color: var(--blue2); opacity: .2; line-height: 1; margin-bottom: 10px; }
  .section-title { font-size: 34px; font-weight: 700; color: var(--white); margin-bottom: 12px; }
  .section-desc  { font-size: 17px; color: var(--muted); }

  /* ── CHANNEL SLIDE ── */
  .channel-slide { max-width: 1060px; margin: 0 auto; width: 100%; }
  .ch-header { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }
  .ch-num {
    background: var(--blue2); color: var(--white);
    font-size: 11px; font-weight: 700; padding: 3px 9px; border-radius: 4px; letter-spacing: 1px;
  }
  .ch-sub { font-size: 11px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: 2px; }
  .ch-err {
    font-size: 11px; font-weight: 700; color: var(--yellow);
    font-family: monospace; background: rgba(254,197,20,.1); padding: 2px 7px; border-radius: 4px;
  }
  .ch-pill {
    font-size: 10px; font-weight: 700; padding: 3px 10px; border-radius: 10px;
    text-transform: uppercase; letter-spacing: .8px; margin-left: auto;
  }
  .ch-pill.fault  { background: rgba(248,81,73,.18);  color: var(--critical); border: 1px solid rgba(248,81,73,.35); }
  .ch-pill.benign { background: rgba(63,185,80,.14);  color: var(--nominal);  border: 1px solid rgba(63,185,80,.35); }
  .ch-name { font-size: 20px; font-weight: 700; color: var(--white); margin-bottom: 18px; }

  /* ── FLOW DIAGRAM ── */
  .flow-wrap {
    display: flex; align-items: flex-start; gap: 0;
    margin-bottom: 18px; overflow-x: auto; padding-bottom: 6px;
  }
  .flow-stage { display: flex; flex-direction: column; align-items: center; min-width: 108px; flex-shrink: 0; }
  .flow-label { font-size: 9px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; white-space: nowrap; }
  .flow-cards { display: flex; flex-direction: column; gap: 4px; }
  .flow-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; padding: 8px 10px; min-width: 100px;
    text-align: center;
    opacity: 0; transform: translateY(10px);
    transition: opacity .35s ease, transform .35s ease;
  }
  .flow-card.visible { opacity: 1; transform: translateY(0); }
  .fc-name { font-size: 11px; font-weight: 600; color: var(--white); line-height: 1.3; }
  .fc-sub  { font-size: 9px;  color: var(--muted); margin-top: 3px; line-height: 1.3; }
  .flow-card.c-trigger  { border-color: var(--muted); }
  .flow-card.c-critical { border-color: var(--critical); background: rgba(248,81,73,.07); }
  .flow-card.c-warning  { border-color: var(--warning);  background: rgba(210,153,34,.07); }
  .flow-card.c-detect   { border-color: var(--blue2);    background: rgba(31,111,235,.08); }
  .flow-card.c-agent    { border-color: var(--yellow);   background: rgba(254,197,20,.06); }
  .flow-card.c-remediate{ border-color: var(--nominal);  background: rgba(63,185,80,.07); }
  .flow-card.c-nominal  { border-color: var(--nominal);  background: rgba(63,185,80,.12); }
  .flow-card.c-isolated { border-color: var(--border);   color: var(--muted); font-style: italic; background: var(--bg); }
  .flow-arrow {
    font-size: 18px; color: var(--muted); display: flex; align-items: center;
    padding: 22px 2px 0; flex-shrink: 0;
  }
  .cloud-dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }

  /* ── DETAIL PANELS ── */
  .detail-row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
  .detail-block { background: var(--bg2); border: 1px solid var(--border); border-radius: 6px; padding: 12px 14px; }
  .detail-label { font-size: 10px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--muted); margin-bottom: 7px; }
  .detail-text  { font-size: 12px; color: var(--text); line-height: 1.65; }
  .mono-block {
    background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    padding: 10px 14px; font-family: 'SF Mono','Fira Code',monospace;
    font-size: 11px; color: var(--text); white-space: pre-wrap; overflow-x: auto;
    line-height: 1.55; margin-bottom: 10px;
  }
  .mono-block .mono-label { font-size: 10px; color: var(--muted); display: block; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 1px; }
  .ph  { color: var(--yellow); }
  .val { color: var(--nominal); }
  .notes-list { list-style: none; }
  .notes-list li {
    display: flex; gap: 8px; padding: 5px 0;
    border-bottom: 1px solid #21262d; font-size: 11px; color: var(--text); line-height: 1.55;
  }
  .notes-list li:last-child { border-bottom: none; }
  .notes-list li::before { content: '▸'; color: var(--blue); flex-shrink: 0; margin-top: 1px; }

  /* ── CHEATSHEET ── */
  .cheatsheet-slide { max-width: 1060px; margin: 0 auto; width: 100%; }
  .cs-table { width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 18px; }
  .cs-table th {
    background: var(--bg2); color: var(--muted); font-size: 10px; font-weight: 700;
    letter-spacing: 1px; text-transform: uppercase; padding: 8px 10px; text-align: left;
    border-bottom: 1px solid var(--border); white-space: nowrap;
  }
  .cs-table td { padding: 6px 10px; border-bottom: 1px solid #21262d; color: var(--text); vertical-align: top; }
  .cs-table tr:hover td { background: var(--bg2); }
  .cs-num  { font-weight: 700; color: var(--blue); white-space: nowrap; }
  .cs-err  { font-family: monospace; font-size: 10px; color: var(--yellow); }
  .cs-aff  { color: var(--critical); }
  .cs-cas  { color: var(--warning); }
  .cs-none { color: var(--muted); font-style: italic; }
  .pill-f { font-size: 9px; font-weight: 700; color: var(--critical); border: 1px solid rgba(248,81,73,.4); padding: 1px 5px; border-radius: 8px; white-space: nowrap; }
  .pill-b { font-size: 9px; font-weight: 700; color: var(--nominal);  border: 1px solid rgba(63,185,80,.4);  padding: 1px 5px; border-radius: 8px; white-space: nowrap; }
  .curl-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .curl-block { background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 12px 16px; }
  .curl-label { font-size: 10px; font-weight: 700; letter-spacing: 1px; color: var(--muted); margin-bottom: 6px; text-transform: uppercase; }
  .curl-cmd { font-family: monospace; font-size: 11px; color: var(--blue); white-space: pre; }

  /* ── BACK BUTTON ── */
  .back-btn {
    display: inline-block; margin-top: 18px; background: transparent; color: var(--yellow);
    border: 1px solid var(--yellow); border-radius: 6px; padding: 8px 20px;
    font-size: 13px; font-weight: 700; cursor: pointer; transition: background .15s;
  }
  .back-btn:hover { background: rgba(254,197,20,.1); }

  /* scrollbar */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-brand"><span>__ICON__</span> ELASTIC OBSERVABILITY</div>
  <div class="topbar-title">__TITLE__ — Chaos Channels</div>
  <div class="topbar-counter" id="counter">1 / __TOTAL__</div>
  <div class="progress-bar" id="progressBar" style="width:__INIT_PROGRESS__%"></div>
</div>

<div class="stage" id="stage"></div>

<div class="bottombar">
  <div class="dots-nav-row">
    <div class="dots-row" id="dotsRow"></div>
    <button class="toc-jump-btn hidden" id="tocJumpBtn" onclick="goTo(1)">☰ TOC</button>
  </div>
  <div class="nav-row">
    <button class="nav-btn" id="prevBtn" onclick="navigate(-1)">← Prev</button>
    <div class="key-hint">← → arrow keys &nbsp;|&nbsp; T = TOC &nbsp;|&nbsp; F = filter in TOC</div>
    <button class="nav-btn" id="nextBtn" onclick="navigate(1)">Next →</button>
  </div>
</div>

<script>
/* ── data ── */
const slides = __SLIDES_DATA__;

const CLOUD_COLORS = {aws:"#FF9900", gcp:"#4285F4", azure:"#0078D4"};
const CLOUD_LABELS = {aws:"AWS",     gcp:"GCP",     azure:"Azure"};

let current = 0;
let animTimer = null;

/* ── utils ── */
function esc(s) {
  return String(s)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;");
}
function cloudDot(prov) {
  const c = CLOUD_COLORS[prov] || "#888";
  return `<span class="cloud-dot" style="background:${c}"></span>`;
}
function slideIndex(type, num) {
  return slides.findIndex(s => s.type === type && (num === undefined || s.num === num));
}

/* ── renderers ── */
function renderTitle(s) {
  const metaCards = Object.entries(s.meta).map(([l,v]) =>
    `<div class="meta-card"><div class="meta-label">${esc(l)}</div><div class="meta-value">${esc(v)}</div></div>`
  ).join("");
  return `<div class="title-slide">
    <div class="title-icon">${esc(s.icon)}</div>
    <div class="title-eyebrow">⚡ Elastic · Chaos Channel Demo Guide</div>
    <div class="title-main">${esc(s.name)}</div>
    <div class="title-sub">${esc(s.subtitle)}</div>
    <div class="title-meta">${metaCards}</div>
  </div>`;
}

function renderToc(s) {
  const tiles = s.summary.map(ch => {
    const idx = slideIndex("channel", ch.num);
    return `<div class="toc-tile" data-fault="${ch.is_fault?1:0}"
        data-search="${esc(ch.name+' '+ch.subsystem+' '+ch.error_type).toLowerCase()}"
        onclick="goTo(${idx})">
      <div class="toc-num">Channel ${String(ch.num).padStart(2,"0")}</div>
      <div class="toc-err">${esc(ch.error_type)}</div>
      <div class="toc-name">${esc(ch.name)}</div>
      <div class="toc-sub">${esc(ch.subsystem)}</div>
    </div>`;
  }).join("");
  return `<div class="toc-slide">
    <div class="slide-eyebrow">TABLE OF CONTENTS — Click any channel to jump · Type to filter</div>
    <input class="toc-filter" id="tocFilter" placeholder="Filter by name, subsystem, or error type…" oninput="filterToc(this.value)">
    <div class="toc-grid" id="tocGrid">${tiles}</div>
  </div>`;
}

function filterToc(val) {
  const v = val.toLowerCase();
  document.querySelectorAll(".toc-tile").forEach(t => {
    t.classList.toggle("toc-hidden", v && !(t.dataset.search||"").includes(v));
  });
}

function renderPlatform(s) {
  const cols = s.clouds.map(cl => {
    const col = CLOUD_COLORS[cl.provider]||"#888";
    const lbl = CLOUD_LABELS[cl.provider]||cl.provider;
    const cards = cl.services.map(sv =>
      `<div class="svc-card" style="border-left:3px solid ${col}">
        <div class="svc-name">${esc(sv.service)}</div>
        <div class="badges">
          <span class="badge badge-sub">${esc(sv.subsystem)}</span>
          ${sv.language ? `<span class="badge badge-lang">${esc(sv.language)}</span>` : ""}
        </div>
        <div style="font-size:10px;color:var(--muted);margin-top:4px">${esc(sv.region||"")}</div>
      </div>`
    ).join("");
    return `<div class="cloud-col" style="border-top:3px solid ${col}">
      <div class="cloud-header">
        <span class="cloud-badge" style="background:${col}">${lbl}</span>
        <span class="cloud-region">${esc(cl.region)}</span>
      </div>
      ${cards}
    </div>`;
  }).join("");
  return `<div class="platform-slide">
    <div class="slide-eyebrow">${esc(s.icon)} ${esc(s.name)} — Service Architecture</div>
    <div class="cloud-grid">${cols}</div>
  </div>`;
}

function renderSection(s) {
  return `<div class="section-break">
    <div class="section-part" style="color:${s.color||"var(--blue2)"}">${esc(s.part)}</div>
    <div class="section-title">${esc(s.title)}</div>
    <div class="section-desc">${esc(s.desc)}</div>
  </div>`;
}

function renderChannel(s) {
  const pillCls = s.is_fault ? "fault" : "benign";
  const pillTxt = s.is_fault ? "⚠ Fault · Agent Required" : "✓ Benign · Self-Healing";

  function svcCards(svcs, cls) {
    if (!svcs || !svcs.length)
      return `<div class="flow-card c-isolated visible"><div class="fc-name">None</div><div class="fc-sub">isolated fault</div></div>`;
    return svcs.map((sv, i) => {
      const sc = (s.svc_cloud||{})[sv]||{};
      const prov = sc.provider||"aws";
      const col  = CLOUD_COLORS[prov]||"#888";
      const lbl  = CLOUD_LABELS[prov]||prov;
      return `<div class="flow-card ${cls}" style="transition-delay:${(i+1)*80}ms">
        <div class="fc-name">${cloudDot(prov)}${esc(sv)}</div>
        <div class="fc-sub">${lbl} · ${esc(sc.region||"")}</div>
      </div>`;
    }).join("");
  }

  const ca  = s.correlation_attr||{};
  const rca = s.rca_clues||[];
  const ex  = s.example_values||{};

  // error_message with placeholders highlighted + example values
  const msgHtml = (s.error_message||"").replace(/\{(\w+)\}/g, (m, k) => {
    const ev = ex[k] !== undefined ? `<span class="val"> (${esc(String(ex[k]))})</span>` : "";
    return `<span class="ph">${esc(m)}</span>${ev}`;
  });

  // investigation notes
  const noteLines = (s.investigation_notes||"").split("\n").filter(l=>l.trim());
  const notesHtml = noteLines.map(l=>`<li>${esc(l.replace(/^\d+\.\s*/,""))}</li>`).join("");

  const descHtml = s.description
    ? `<div class="detail-block"><div class="detail-label">📋 Description</div><div class="detail-text">${esc(s.description)}</div></div>` : "";
  const notesBlock = notesHtml
    ? `<div class="detail-block"><div class="detail-label">▸ Stage Notes</div><ul class="notes-list">${notesHtml}</ul></div>` : "";

  return `<div class="channel-slide">
    <div class="ch-header">
      <span class="ch-num">CH ${String(s.num).padStart(2,"0")}</span>
      <span class="ch-sub">${esc(s.subsystem)}</span>
      <span class="ch-err">${esc(s.error_type)}</span>
      <span class="ch-pill ${pillCls}">${pillTxt}</span>
    </div>
    <div class="ch-name">${esc(s.name)}</div>

    <div class="flow-wrap">
      <div class="flow-stage">
        <div class="flow-label">⚡ Trigger</div>
        <div class="flow-cards">
          <div class="flow-card c-trigger" style="transition-delay:0ms">
            <div class="fc-name">CH ${s.num}</div>
            <div class="fc-sub">${esc(s.sensor_type)}</div>
          </div>
        </div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">💥 Affected</div>
        <div class="flow-cards">${svcCards(s.affected_services,"c-critical")}</div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">⚠️ Cascade</div>
        <div class="flow-cards">${svcCards(s.cascade_services,"c-warning")}</div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">🔍 ES|QL Detect</div>
        <div class="flow-cards">
          <div class="flow-card c-detect" style="transition-delay:200ms">
            <div class="fc-name">${esc(s.error_type)}</div>
            <div class="fc-sub">${ca.key ? esc(ca.key)+" = "+esc(ca.value) : "significant event"}</div>
          </div>
        </div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">🤖 AI Agent</div>
        <div class="flow-cards">
          <div class="flow-card c-agent" style="transition-delay:280ms">
            <div class="fc-name">RCA Investigation</div>
            <div class="fc-sub">${rca.length
              ? esc(rca.slice(0,2).join(", "))+(rca.length>2?` +${rca.length-2} more`:"")
              : "pattern analysis"}</div>
          </div>
        </div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">🔧 Remediate</div>
        <div class="flow-cards">
          <div class="flow-card c-remediate" style="transition-delay:360ms">
            <div class="fc-name">${esc(s.remediation_action)}</div>
            <div class="fc-sub">POST /api/remediate/${s.num}</div>
          </div>
        </div>
      </div>
      <div class="flow-arrow">→</div>

      <div class="flow-stage">
        <div class="flow-label">✅ ${esc(s.nominal_label)}</div>
        <div class="flow-cards">
          <div class="flow-card c-nominal" style="transition-delay:440ms">
            <div class="fc-name">${esc(s.nominal_label)}</div>
            <div class="fc-sub">all services green</div>
          </div>
        </div>
      </div>
    </div>

    <div class="detail-row">${descHtml}${notesBlock}</div>

    ${msgHtml ? `<div class="mono-block"><span class="mono-label">Error Signature</span>${msgHtml}</div>` : ""}
  </div>`;
}

function renderCheatsheet(s) {
  const rows = s.channels.map(ch => {
    const pill = ch.is_fault
      ? `<span class="pill-f">FAULT</span>`
      : `<span class="pill-b">BENIGN</span>`;
    const aff = ch.affected.length ? `<span class="cs-aff">${esc(ch.affected.join(", "))}</span>` : `<span class="cs-none">—</span>`;
    const cas = ch.cascade.length  ? `<span class="cs-cas">${esc(ch.cascade.join(", "))}</span>`  : `<span class="cs-none">isolated</span>`;
    return `<tr>
      <td class="cs-num">${pill} ${String(ch.num).padStart(2,"0")}</td>
      <td>${esc(ch.name)}</td>
      <td>${esc(ch.subsystem)}</td>
      <td class="cs-err">${esc(ch.error_type)}</td>
      <td>${aff}</td>
      <td>${cas}</td>
    </tr>`;
  }).join("");
  return `<div class="cheatsheet-slide">
    <div class="slide-eyebrow">⚡ All 20 Channels — Quick Reference</div>
    <table class="cs-table">
      <thead><tr><th>#</th><th>Name</th><th>Subsystem</th><th>Error Type</th><th>Affected</th><th>Cascade</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="curl-row">
      <div class="curl-block">
        <div class="curl-label">Trigger fault</div>
        <div class="curl-cmd">curl -X POST http://&lt;host&gt;/api/chaos/trigger \
  -H 'Content-Type: application/json' \
  -d '{"channel": N}'</div>
      </div>
      <div class="curl-block">
        <div class="curl-label">Resolve fault</div>
        <div class="curl-cmd">curl -X POST http://&lt;host&gt;/api/remediate/N</div>
      </div>
    </div>
    <button class="back-btn" onclick="goTo(1)">↑ Back to Contents</button>
  </div>`;
}

/* ── main render ── */
function renderSlide(idx) {
  const s = slides[idx];
  const stage = document.getElementById("stage");
  if      (s.type==="title")       stage.innerHTML = renderTitle(s);
  else if (s.type==="toc")         stage.innerHTML = renderToc(s);
  else if (s.type==="platform")    stage.innerHTML = renderPlatform(s);
  else if (s.type==="section")     stage.innerHTML = renderSection(s);
  else if (s.type==="channel")     stage.innerHTML = renderChannel(s);
  else if (s.type==="cheatsheet")  stage.innerHTML = renderCheatsheet(s);
  // Stagger-animate flow cards after render
  if (s.type==="channel") {
    if (animTimer) clearTimeout(animTimer);
    animTimer = setTimeout(()=>{
      document.querySelectorAll(".flow-card").forEach(c=>c.classList.add("visible"));
    }, 40);
  }
}

function dotKind(s) {
  if (s.type==="channel") return s.is_fault ? "fault" : "benign";
  return s.type;
}

function buildDots() {
  const row = document.getElementById("dotsRow");
  row.innerHTML = slides.map((s,i) =>
    `<button class="dot-btn${i===current?" active":""}" data-kind="${dotKind(s)}"
      onclick="goTo(${i})" title="${s.type==="channel"?`Ch ${s.num}: ${esc(s.name)}`:s.type}"></button>`
  ).join("");
}

function updateUI() {
  renderSlide(current);
  buildDots();
  const hide = current<=1;
  document.getElementById("tocJumpBtn").classList.toggle("hidden", hide);
  document.getElementById("counter").textContent = `${current+1} / ${slides.length}`;
  document.getElementById("progressBar").style.width = `${((current+1)/slides.length*100).toFixed(2)}%`;
  document.getElementById("prevBtn").disabled = current===0;
  document.getElementById("nextBtn").disabled = current===slides.length-1;
}

function goTo(idx) {
  if (idx<0||idx>=slides.length||idx===current) return;
  const stage = document.getElementById("stage");
  stage.classList.add("fade");
  setTimeout(()=>{ current=idx; updateUI(); stage.classList.remove("fade"); }, 180);
}

function navigate(dir) { goTo(current+dir); }

document.addEventListener("keydown", e=>{
  if (e.key==="ArrowRight"||e.key==="ArrowDown") navigate(1);
  if (e.key==="ArrowLeft" ||e.key==="ArrowUp")   navigate(-1);
  if ((e.key==="t"||e.key==="T") && current!==1) goTo(1);
  if (e.key==="f" && current===1) {
    const f=document.getElementById("tocFilter");
    if (f){f.focus();e.preventDefault();}
  }
});

updateUI();
</script>
</body>
</html>"""


def render_html(slides: list, scenario) -> str:
    sname   = scenario.scenario_name
    icon    = getattr(scenario, "scenario_icon", "⚡")
    total   = len(slides)
    init_p  = round(100 / total, 2)

    slides_json = json.dumps(slides, ensure_ascii=False, indent=2)

    return (
        _TEMPLATE
        .replace("__TITLE__",         html_mod.escape(sname))
        .replace("__ICON__",          html_mod.escape(icon))
        .replace("__TOTAL__",         str(total))
        .replace("__INIT_PROGRESS__", str(init_p))
        .replace("__SLIDES_DATA__",   slides_json)
    )


# ── main ─────────────────────────────────────────────────────────────────────

def generate(scenario_id: str, out_path: Path) -> int:
    """Generate HTML for one scenario. Returns slide count (0 = error)."""
    scenario_dir = REPO_ROOT / "scenarios" / scenario_id
    if not scenario_dir.exists():
        print(f"  ✗  {scenario_id}: no directory at {scenario_dir}", file=sys.stderr)
        return 0
    scenario = get_scenario(scenario_id)
    services = load_services(scenario_dir)
    channels = load_channels(scenario_dir)
    if not channels:
        print(f"  !  {scenario_id}: no channel YAMLs found", file=sys.stderr)
    slides    = build_slides(scenario, channels, services)
    html      = render_html(slides, scenario)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return len(slides)


def main():
    parser = argparse.ArgumentParser(description="Generate Chaos Channel Demo Guide HTML")
    parser.add_argument("scenario", nargs="?",
                        help="Scenario ID (e.g. relief, space, healthcare)")
    parser.add_argument("--all", action="store_true",
                        help="Generate for every known scenario")
    parser.add_argument("--out", type=Path,
                        help="Output file path (single scenario, overrides default)")
    parser.add_argument("--out-dir", type=Path,
                        help="Base dir for --all (default: /tmp/channel-guides)")
    args = parser.parse_args()

    if args.all:
        scenarios = list_scenarios()
        # list_scenarios may return objects or dicts — handle both
        ids = []
        for s in scenarios:
            ids.append(s.scenario_id if hasattr(s, "scenario_id") else s["scenario_id"])
        base = args.out_dir or Path("/tmp/channel-guides")
        for sid in sorted(ids):
            out = base / sid / "chaos-channels.html"
            try:
                n = generate(sid, out)
                if n:
                    print(f"  ✓  {sid:<22} {n} slides  →  {out}")
            except Exception as exc:
                print(f"  ✗  {sid:<22} ERROR: {exc}", file=sys.stderr)

    elif args.scenario:
        sid = args.scenario
        default_out = REPO_ROOT / "scenarios" / sid / "demo-guide" / "chaos-channels.html"
        out = args.out or default_out
        try:
            n = generate(sid, out)
            if n:
                print(f"✓  {n} slides  →  {out}")
        except Exception as exc:
            print(f"✗  ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
