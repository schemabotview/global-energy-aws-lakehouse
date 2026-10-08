from decimal import Decimal

import pandas as pd
import pytest

from generator.model import Config, build_scenario
from generator.sinks import DmsImitationSink

CFG = Config(customers=60)


@pytest.fixture(scope="module")
def sc():
    return build_scenario(CFG)


def _independent_totals(sc):
    """Recompute the control totals from the generated rows only, applying the documented rules."""
    rd = pd.concat([sc.day1["meter_reading"], sc.day2["meter_reading"].drop(columns="Op")], ignore_index=True)
    meters = set(sc.day1["meter"]["meter_id"]) | set(sc.day2["meter"].query("Op == 'I'")["meter_id"])
    ok = rd["consumption_kwh"].map(lambda v: v is not None and v >= 0) & (rd["reading_type"] == "HALF_HOURLY") \
        & rd["meter_id"].isin(meters)
    rd = rd[ok].sort_values(["reading_id", "version"]).drop_duplicates("reading_id", keep="last").set_index("reading_id")
    for c in sc.day2["reading_correction"].itertuples():
        if c.corrected_at > rd.loc[c.reading_id, "source_updated_at"]:
            rd.loc[c.reading_id, "consumption_kwh"] = c.replacement_kwh
    cal = sc.day1["settlement_calendar"].set_index("start_utc")["settlement_date"]
    rd["date"] = rd["reading_ts"].map(lambda t: cal[t])
    return rd.groupby("date")["consumption_kwh"].agg(["sum", "count"])


def test_dst_day_has_fifty_periods(sc):
    assert sc.expected["day1"]["periods"] == 48
    assert sc.expected["day2"]["periods"] == 50


def test_control_totals_match_independent_recomputation(sc):
    totals = _independent_totals(sc)
    d1, d2 = CFG.day1, CFG.day2
    final = sc.day2["reconciliation_control"].set_index("period")
    assert final.loc[d1, "expected_total"] == totals.loc[d1, "sum"]
    assert final.loc[d2, "expected_total"] == totals.loc[d2, "sum"]
    assert final.loc[d1, "expected_count"] == totals.loc[d1, "count"]
    assert final.loc[d1, "control_version"] == 2  # re-stated after corrections
    # the day-1 control as first published is lower than the corrected one (corrections increase kWh)
    first = sc.day1["reconciliation_control"].iloc[0]
    assert first["expected_total"] < final.loc[d1, "expected_total"]


def test_defects_and_changes_are_present(sc):
    for day in ("day1", "day2"):
        assert all(v >= 1 for v in sc.expected[day]["defects"].values())
    assert sc.expected["day2"]["corrections"] >= 1
    assert set(sc.day2["meter_assignment"]["Op"]) == {"I", "U"}
    assert set(sc.day2["customer"]["Op"]) == {"U"}


def test_bad_control_inflates_only_day_two():
    good = build_scenario(CFG).day2["reconciliation_control"].set_index("period")
    bad = build_scenario(Config(customers=60, bad_control=True)).day2["reconciliation_control"].set_index("period")
    d1, d2 = CFG.day1, CFG.day2
    assert bad.loc[d1, "expected_total"] == good.loc[d1, "expected_total"]
    assert bad.loc[d2, "expected_total"] > good.loc[d2, "expected_total"] * Decimal("1.0099")


def test_dms_imitation_files(tmp_path, sc):
    import pyarrow.parquet as pq

    sink = DmsImitationSink(tmp_path, "baseline")
    sink.write_day1(sc.day1, sc.day1_load_ts)
    sink.write_day2(sc.day2, sc.day2_load_ts)
    t = pq.read_table(tmp_path / "meter_reading" / "LOAD00000001.parquet")
    assert t.column_names[0] == "Op" and t.column_names[-1] == "dms_commit_ts"
    assert set(t["Op"].to_pylist()) == {"I"}
    cdc = list((tmp_path / "meter_reading").glob("2026/10/26/*.parquet"))
    assert len(cdc) == 1


def test_schema_change_scenarios(tmp_path, sc):
    import pyarrow.parquet as pq

    DmsImitationSink(tmp_path / "a", "additive").write_day2(sc.day2, sc.day2_load_ts)
    DmsImitationSink(tmp_path / "b", "breaking_unit").write_day2(sc.day2, sc.day2_load_ts)
    meter = next((tmp_path / "a" / "meter").glob("**/*.parquet"))
    reading = next((tmp_path / "b" / "meter_reading").glob("**/*.parquet"))
    assert "firmware_version" in pq.read_schema(meter).names
    names = pq.read_schema(reading).names
    assert "consumption_wh" in names and "consumption_kwh" not in names
