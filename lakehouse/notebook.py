"""Shared notebook plumbing: read job parameters (widgets) into run_id + Settings."""
from __future__ import annotations

from lakehouse.config import Settings

DEFAULTS = (("run_id", ""), ("landing_uri", ""), ("ctl_uri", ""), ("quarantine_uri", ""), ("recon_threshold_pct", "0.01"))


def params(dbutils) -> tuple[str, Settings]:
    for name, default in DEFAULTS:
        dbutils.widgets.text(name, default)
    g = dbutils.widgets.get
    if not g("run_id"):
        raise ValueError("run_id is required (the Step Functions execution name)")
    return g("run_id"), Settings(
        landing_uri=g("landing_uri"), ctl_uri=g("ctl_uri"), quarantine_uri=g("quarantine_uri"),
        recon_threshold_pct=float(g("recon_threshold_pct")),
    )
