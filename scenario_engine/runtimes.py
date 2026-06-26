"""Shared runtime metadata for each supported programming language.

Single source of truth used by both the telemetry generator (app/telemetry.py)
and the Streams knowledge-indicator deployer (elastic_config/deployer_streams.py).
When adding a new language, add an entry here and it will flow into both the
emitted telemetry *and* the deployed knowledge indicators automatically.
"""

from __future__ import annotations

# Mapping of telemetry SDK language identifier → runtime metadata.
# Fields:
#   display_name            — human-readable name shown in knowledge indicator titles
#   runtime_name            — process.runtime.name value emitted in telemetry;
#                             also used as meta.runtime_name in technology indicators
#   version                 — clean semver (e.g. "1.79.0"), used in indicator
#                             props.version and meta.runtime_version
#   process_runtime_version — exact string emitted as process.runtime.version in
#                             OTLP (may differ from version, e.g. "go1.22.4")
#   process_runtime_description — process.runtime.description value in OTLP
RUNTIME_BY_LANGUAGE: dict[str, dict[str, str]] = {
    "java": {
        "display_name": "Java",
        "runtime_name": "OpenJDK Runtime Environment",
        "version": "21.0.5",
        "process_runtime_version": "21.0.5+11-LTS",
        "process_runtime_description": (
            "Eclipse Adoptium OpenJDK 64-Bit Server VM 21.0.5+11-LTS"
        ),
    },
    "python": {
        "display_name": "Python",
        "runtime_name": "CPython",
        "version": "3.12.3",
        "process_runtime_version": "3.12.3",
        "process_runtime_description": "CPython 3.12.3",
    },
    "go": {
        "display_name": "Go",
        "runtime_name": "go",
        "version": "1.22.4",
        "process_runtime_version": "go1.22.4",
        "process_runtime_description": "go1.22.4 linux/amd64",
    },
    "dotnet": {
        "display_name": ".NET",
        "runtime_name": ".NET",
        "version": "8.0.6",
        "process_runtime_version": "8.0.6",
        "process_runtime_description": ".NET 8.0.6",
    },
    "rust": {
        "display_name": "Rust",
        "runtime_name": "rustc",
        "version": "1.79.0",
        "process_runtime_version": "1.79.0",
        "process_runtime_description": "rustc 1.79.0",
    },
    "cpp": {
        "display_name": "C++",
        "runtime_name": "gcc",
        "version": "13.2.0",
        "process_runtime_version": "13.2.0",
        "process_runtime_description": "GCC 13.2.0",
    },
}
