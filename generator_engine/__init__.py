"""Generator engine — registry, SPEC contract, shared helpers, schema versioning.

User generator content lives under ``generators/<id>/`` (PVC-mounted in k8s).
This package is image-immutable and must not be modified via import/export.
"""
