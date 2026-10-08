"""Run the whole pipeline locally on a DMS imitation (no AWS, no Postgres). Not run by CI by default.

  python scripts/run_local.py --workdir .local/run --scenario baseline
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

os.environ.setdefault("LAKE_ENV", "local")

from generator.model import Config, build_scenario  # noqa: E402
from generator.sinks import SCENARIOS, DmsImitationSink  # noqa: E402
from lakehouse import contract  # noqa: E402
from lakehouse.config import Settings  # noqa: E402
from lakehouse.pipeline import run_batch  # noqa: E402
from lakehouse.spark import get_spark  # noqa: E402
from lakehouse.store import LocalStore  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--workdir", default=".local/run")
    p.add_argument("--scenario", choices=SCENARIOS, default="baseline")
    p.add_argument("--bad-control", action="store_true")
    a = p.parse_args()

    root = Path(a.workdir)
    shutil.rmtree(root, ignore_errors=True)
    sc = build_scenario(Config(bad_control=a.bad_control))
    landing, ctl, quar = root / "dms" / "src", root / "ctl", root / "quarantine"
    sink = DmsImitationSink(landing, a.scenario)
    settings = Settings(str(landing), str(ctl), str(quar))
    spark = get_spark(str(root / "warehouse"))

    for day, (run_id, write) in enumerate([("run-day1", lambda: sink.write_day1(sc.day1, sc.day1_load_ts)),
                                           ("run-day2", lambda: sink.write_day2(sc.day2, sc.day2_load_ts))], 1):
        write()
        m = contract.run_check(LocalStore(landing), LocalStore(quar), LocalStore(ctl), run_id)
        print(f"day {day}: contract passed={ {t: len(k) for t, k in m['passed'].items()} } "
              f"quarantined={len(m['quarantined'])} additive={m['additive']}")
        print(run_batch(spark, settings, run_id))


if __name__ == "__main__":
    main()
