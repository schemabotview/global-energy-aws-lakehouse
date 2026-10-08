"""Generate synthetic data.

  python -m generator.cli postgres --dsn "$SRC_DSN" --day 1 --reset     # seed the RDS source (DMS full load)
  python -m generator.cli postgres --dsn "$SRC_DSN" --day 2             # apply day-2 changes (DMS CDC)
  python -m generator.cli dms-sim  --out .local/dms/src --scenario baseline   # local imitation of DMS output
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

from generator.model import Config, build_scenario
from generator.sinks import SCENARIOS, DmsImitationSink, PostgresSink, write_expected

DDL = Path(__file__).resolve().parents[1] / "sql" / "postgres" / "001_schema.sql"


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--customers", type=int, default=300)
    p.add_argument("--day1", type=dt.date.fromisoformat, default=dt.date(2026, 10, 24))
    p.add_argument("--bad-control", action="store_true", help="inflate the day-2 control total by 1%%")
    p.add_argument("--expected-out", default=None, help="write the expected totals / defect counts here")
    sub = p.add_subparsers(dest="sink", required=True)

    pg = sub.add_parser("postgres")
    pg.add_argument("--dsn", default=os.getenv("SRC_DSN"))
    pg.add_argument("--day", choices=["1", "2", "both"], required=True)
    pg.add_argument("--reset", action="store_true", help="drop and recreate schema src first (day 1 only)")

    sim = sub.add_parser("dms-sim")
    sim.add_argument("--out", required=True)
    sim.add_argument("--scenario", choices=SCENARIOS, default="baseline")
    sim.add_argument("--days", choices=["1", "2", "both"], default="both")

    a = p.parse_args(argv)
    sc = build_scenario(Config(seed=a.seed, customers=a.customers, day1=a.day1, bad_control=a.bad_control))
    if a.expected_out:
        write_expected(sc.expected, a.expected_out)

    if a.sink == "postgres":
        if not a.dsn:
            p.error("--dsn or SRC_DSN is required")
        sink = PostgresSink(a.dsn)
        try:
            if a.day in ("1", "both"):
                sink.create_schema(DDL, reset=a.reset)
                sink.load_day1(sc.day1)
            if a.day in ("2", "both"):
                sink.apply_day2(sc.day2)
        finally:
            sink.close()
    else:
        sink = DmsImitationSink(a.out, a.scenario)
        if a.days in ("1", "both"):
            sink.write_day1(sc.day1, sc.day1_load_ts)
        if a.days in ("2", "both"):
            sink.write_day2(sc.day2, sc.day2_load_ts)


if __name__ == "__main__":
    main()
