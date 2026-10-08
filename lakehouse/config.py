"""Runtime settings and naming."""
from __future__ import annotations

import os
from dataclasses import dataclass

CATALOG = os.getenv("LAKE_CATALOG", "lake")
NAMESPACES = ("bronze", "silver", "gold", "ctl")


def fq(namespace: str, name: str) -> str:
    return f"{CATALOG}.{namespace}.{name}"


@dataclass(frozen=True)
class Settings:
    """Where the pipeline reads and writes outside Iceberg.

    landing_uri     DMS output root, e.g. s3://<bucket>/dms/src  (one folder per table underneath)
    ctl_uri         control files: contract state, manifests, quarantine flags, e.g. s3://<bucket>/ctl
    quarantine_uri  where breaking-schema files are moved, e.g. s3://<bucket>/quarantine
    recon_threshold_pct  allowed variance between Gold and the billing control total, in percent
    """

    landing_uri: str
    ctl_uri: str
    quarantine_uri: str
    recon_threshold_pct: float = 0.01

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            landing_uri=os.environ["LAKE_LANDING_URI"],
            ctl_uri=os.environ["LAKE_CTL_URI"],
            quarantine_uri=os.environ["LAKE_QUARANTINE_URI"],
            recon_threshold_pct=float(os.getenv("LAKE_RECON_THRESHOLD_PCT", "0.01")),
        )
