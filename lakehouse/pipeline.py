"""The whole batch in one call. Used by the local runner and tests; on Databricks the same steps run as separate
notebook tasks (databricks/notebooks) so each can be retried on its own."""
from __future__ import annotations

from lakehouse import bronze, dq, gold, reconcile, silver
from lakehouse.config import Settings
from lakehouse.contract import held_tables
from lakehouse.spark import ensure_catalog_objects
from lakehouse.store import open_store


def run_batch(spark, settings: Settings, run_id: str) -> dict:
    ensure_catalog_objects(spark)
    skip = held_tables(open_store(settings.ctl_uri))
    out = {"bronze": bronze.run(spark, settings, run_id)}
    out["silver"] = silver.run(spark, run_id, skip)
    gold.build_dims(spark)
    out["facts"] = gold.build_facts(spark, run_id)
    out["dq"] = dq.run(spark, run_id)
    out["reconciliation"] = reconcile.reconcile(spark, run_id, settings.recon_threshold_pct)
    out["published_dates"] = reconcile.publish(spark, run_id)
    return out
