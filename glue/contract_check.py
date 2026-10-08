"""Glue job: schema contract gate for new DMS files. Writes ctl/manifests/<run_id>.json.

Arguments: --run_id, --bucket, --landing_prefix (default dms/src), --ctl_prefix (default ctl),
           --quarantine_prefix (default quarantine). Needs lakehouse.zip on --extra-py-files and pyarrow.
Fails the job after writing the manifest if any file was quarantined, so Step Functions alerts a person.
"""
import sys

from awsglue.utils import getResolvedOptions

from lakehouse.contract import run_check
from lakehouse.store import S3Store

args = getResolvedOptions(sys.argv, ["run_id", "bucket"])
opt = {"landing_prefix": "dms/src", "ctl_prefix": "ctl", "quarantine_prefix": "quarantine"}
for k in opt:
    if f"--{k}" in sys.argv:
        opt[k] = getResolvedOptions(sys.argv, [k])[k]

bucket = args["bucket"]
manifest = run_check(
    landing=S3Store(bucket, opt["landing_prefix"]),
    quarantine=S3Store(bucket, opt["quarantine_prefix"]),
    ctl=S3Store(bucket, opt["ctl_prefix"]),
    run_id=args["run_id"],
)
print({t: len(k) for t, k in manifest["passed"].items()}, "additive:", manifest["additive"])
if manifest["has_quarantine"]:
    raise RuntimeError(f"SCHEMA_QUARANTINE: {manifest['quarantined']}")
