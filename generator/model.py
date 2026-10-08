"""Deterministic synthetic scenario.

Day 1 is the initial state of the source database (what the DMS full load sees).
Day 2 is a change set (what DMS CDC sees): new readings, corrections to day-1 readings, a tariff change,
a customer region change, new meters and a meter status change. Day 2 is 2026-10-25 by default, the UK
clock-change day, which has 50 half-hour settlement periods.

Billing control totals are computed here, independently of the pipeline, so the pipeline's reconciliation
is a real check and not a self-comparison.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

LONDON = ZoneInfo("Europe/London")
UTC = dt.timezone.utc
OPEN_END = dt.date(9999, 12, 31)
HALF_HOUR = dt.timedelta(minutes=30)
REGIONS = ["SOUTH_EAST", "MIDLANDS", "LONDON", "NORTH_WEST", "SCOTLAND", "WALES"]
TARIFFS = [
    ("TRF-STD-01", "Standard", "STANDARD"),
    ("TRF-ECO7-02", "Economy 7", "ECO7"),
    ("TRF-BIZ-03", "Business Tier 1", "BUSINESS_TIER1"),
    ("TRF-GRN-04", "Green", "GREEN"),
]


@dataclass(frozen=True)
class Config:
    seed: int = 42
    customers: int = 300
    meters_per_customer: float = 1.2
    day1: dt.date = dt.date(2026, 10, 24)
    defect_rate: float = 0.004          # per defect type, as a share of the day's readings
    tariff_change_rate: float = 0.02
    customer_change_rate: float = 0.01
    new_meter_rate: float = 0.02
    correction_rate: float = 0.005
    bad_control: bool = False           # inflate the day-2 control total by 1% to prove the gate blocks

    @property
    def day2(self) -> dt.date:
        return self.day1 + dt.timedelta(days=1)


@dataclass
class Scenario:
    config: Config
    day1: dict[str, pd.DataFrame]       # table -> rows (initial state)
    day2: dict[str, pd.DataFrame]       # table -> change rows with an "Op" column (I / U / D)
    day1_load_ts: dt.datetime           # naive UTC
    day2_load_ts: dt.datetime
    expected: dict


def utc(d: dt.date, hour: int = 0, minute: int = 0) -> dt.datetime:
    """Naive datetime that is UTC by convention (the source columns are `timestamp`, not `timestamptz`)."""
    return dt.datetime.combine(d, dt.time(hour, minute))


def dec(milli) -> Decimal | None:
    if milli is None or (isinstance(milli, float) and np.isnan(milli)):
        return None
    return Decimal(int(milli)).scaleb(-3)


def settlement_calendar(first: dt.date, last: dt.date) -> pd.DataFrame:
    rows = []
    d = first
    while d <= last:
        s = dt.datetime.combine(d, dt.time(0), tzinfo=LONDON).astimezone(UTC)
        e = dt.datetime.combine(d + dt.timedelta(days=1), dt.time(0), tzinfo=LONDON).astimezone(UTC)
        n = int((e - s) / HALF_HOUR)
        for p in range(n):
            ps = s + p * HALF_HOUR
            rows.append({
                "settlement_date": d, "period": p + 1,
                "start_utc": ps.replace(tzinfo=None), "end_utc": (ps + HALF_HOUR).replace(tzinfo=None),
                "local_start": ps.astimezone(LONDON).replace(tzinfo=None),
                "duration_minutes": 30, "dst_flag": n != 48, "calendar_version": 1,
            })
        d += dt.timedelta(days=1)
    return pd.DataFrame(rows)


def _make_readings(rng, meter_ids: list[str], cal_day: pd.DataFrame) -> pd.DataFrame:
    periods, meters = len(cal_day), len(meter_ids)
    base = rng.lognormal(-1.0, 0.5, meters)
    shape = 0.6 + 0.4 * np.sin(np.linspace(0, 2 * np.pi, periods, endpoint=False) - np.pi / 2)
    noise = rng.normal(1.0, 0.08, (meters, periods)).clip(0.3, None)
    milli = np.rint(base[:, None] * shape[None, :] * noise * 1000).astype(np.int64)
    rd = pd.DataFrame({
        "meter_id": np.repeat(np.array(meter_ids, dtype=object), periods),
        "reading_ts": pd.to_datetime(cal_day["start_utc"].tolist() * meters),
    })
    rd["interval_end"] = rd["reading_ts"] + HALF_HOUR
    rd["reading_id"] = [f"{m}|{t:%Y%m%dT%H%MZ}" for m, t in zip(rd["meter_id"], rd["reading_ts"], strict=True)]
    rd["version"] = 1
    rd["kwh_m"] = milli.ravel().astype(float)
    rd["reading_type"] = "HALF_HOURLY"
    rd["quality_code"] = np.where(rng.random(len(rd)) < 0.03, "E", "A")
    rd["source_updated_at"] = rd["interval_end"] + pd.to_timedelta(rng.integers(300, 1500, len(rd)), unit="s")
    return rd


def _inject_defects(rng, rd: pd.DataFrame, rate: float) -> tuple[pd.DataFrame, dict]:
    """Bad rows the pipeline must reject, plus version replays that must win over version 1."""
    k = max(1, int(len(rd) * rate))
    idx = rng.permutation(len(rd))
    null_i, neg_i, type_i, replay_i, orphan_i = (idx[i * k:(i + 1) * k] for i in range(5))
    rd = rd.copy()
    rd.loc[rd.index[null_i], "kwh_m"] = np.nan
    rd.loc[rd.index[neg_i], "kwh_m"] = -rd.loc[rd.index[neg_i], "kwh_m"].abs() - 1
    rd.loc[rd.index[type_i], "reading_type"] = "UNKNOWN"
    replay = rd.iloc[replay_i].copy()
    replay["version"] = 2
    replay["kwh_m"] = np.rint(replay["kwh_m"] * 1.1)
    replay["source_updated_at"] = replay["source_updated_at"] + HALF_HOUR
    orphan = rd.iloc[orphan_i].copy()
    orphan["meter_id"] = [f"MTR-X{i:05d}" for i in range(len(orphan))]
    orphan["reading_id"] = [f"{m}|{t:%Y%m%dT%H%MZ}" for m, t in zip(orphan["meter_id"], orphan["reading_ts"], strict=True)]
    out = pd.concat([rd, replay, orphan], ignore_index=True)
    return out, {"null_consumption": k, "negative_consumption": k, "invalid_reading_type": k,
                 "newer_version_replays": k, "unknown_meter": k}


def _final_rows(rd: pd.DataFrame, meters: set[str]) -> pd.DataFrame:
    """What a correct pipeline keeps: valid rows, latest version per reading."""
    ok = (rd["kwh_m"].notna() & (rd["kwh_m"] >= 0) & (rd["reading_type"] == "HALF_HOURLY")
          & rd["meter_id"].isin(meters))
    return (rd[ok].sort_values(["reading_id", "version"]).drop_duplicates("reading_id", keep="last")
            .reset_index(drop=True))


def _readings_frame(rd: pd.DataFrame) -> pd.DataFrame:
    out = rd[["reading_id", "meter_id", "reading_ts", "interval_end", "version", "reading_type",
              "quality_code", "source_updated_at"]].copy()
    out["consumption_kwh"] = [dec(v) for v in rd["kwh_m"]]
    cols = ["reading_id", "meter_id", "reading_ts", "interval_end", "version", "consumption_kwh",
            "reading_type", "quality_code", "source_updated_at"]
    return out[cols]


def _control(period: dt.date, total_m: int, count: int, run_id: str, version: int, created: dt.datetime,
             inflate: bool = False) -> dict:
    if inflate:
        total_m = round(total_m * 1.01)
    return {
        "source_name": "BILLING", "period": period, "control": "CONSUMPTION_KWH", "run_id": run_id,
        "expected_count": int(count), "expected_total": dec(total_m), "unit": "kWh",
        "source_cutoff": utc(period + dt.timedelta(days=1)), "created_at": created, "control_version": version,
    }


def _with_op(df: pd.DataFrame, op: str) -> pd.DataFrame:
    out = df.copy()
    out.insert(0, "Op", op)
    return out


DEFAULT_CONFIG = Config()


def build_scenario(cfg: Config = DEFAULT_CONFIG) -> Scenario:
    rng = np.random.default_rng(cfg.seed)
    d1, d2 = cfg.day1, cfg.day2
    created = utc(d1 - dt.timedelta(days=400))
    n_c = cfg.customers
    n_m = int(n_c * cfg.meters_per_customer)

    # ---- reference data (day 1 snapshot)
    tariff = pd.DataFrame([{
        "tariff_id": t, "name": n, "tariff_type": ty, "currency": "GBP",
        "valid_from": d1 - dt.timedelta(days=1000), "valid_to": OPEN_END, "source_updated_at": created,
    } for t, n, ty in TARIFFS])
    cust_type = rng.choice(["DOMESTIC", "COMMERCIAL"], n_c, p=[0.9, 0.1]).tolist()
    customer = pd.DataFrame({
        "customer_id": [f"CUST-{i:06d}" for i in range(n_c)],
        "customer_type": cust_type,
        "region": rng.choice(REGIONS, n_c).tolist(),
        "name": [f"Customer {i}" for i in range(n_c)],
        "address": [f"{i} Example Street" for i in range(n_c)],
        "valid_from": d1 - dt.timedelta(days=400),
        "source_updated_at": created,
    })
    account = pd.DataFrame({
        "account_id": [f"ACC-{i:06d}" for i in range(n_c)],
        "customer_id": customer["customer_id"], "account_ref": [f"AR{i:06d}" for i in range(n_c)],
        "status": "OPEN", "opened_at": d1 - dt.timedelta(days=400), "closed_at": None,
        "source_updated_at": created,
    })
    supply_point = pd.DataFrame({
        "supply_point_id": [f"SP-{i:06d}" for i in range(n_m)],
        "supply_ref": [f"SR{i:06d}" for i in range(n_m)],
        "region": rng.choice(REGIONS, n_m).tolist(),
        "grid_zone": rng.choice(["GZ1", "GZ2", "GZ3"], n_m).tolist(),
        "supply_type": "ELECTRICITY", "status": "ACTIVE", "source_updated_at": created,
    })
    meter = pd.DataFrame({
        "meter_id": [f"MTR-{i:06d}" for i in range(n_m)],
        "supply_point_id": supply_point["supply_point_id"],
        "serial_no": [f"SN{i:08d}" for i in range(n_m)],
        "meter_type": rng.choice(["SMART", "ADVANCED"], n_m).tolist(),
        "status": "ACTIVE", "installed_at": d1 - dt.timedelta(days=300), "source_updated_at": created,
    })

    def tariff_for(ctype: str) -> str:
        if ctype == "COMMERCIAL":
            return "TRF-BIZ-03"
        return str(rng.choice(["TRF-STD-01", "TRF-ECO7-02", "TRF-GRN-04"]))

    owner = [i % n_c for i in range(n_m)]
    assignment = pd.DataFrame({
        "assignment_id": [f"ASG-{i:06d}" for i in range(n_m)],
        "meter_id": meter["meter_id"],
        "account_id": [account["account_id"][c] for c in owner],
        "supply_point_id": supply_point["supply_point_id"],
        "tariff_id": [tariff_for(cust_type[c]) for c in owner],
        "valid_from": d1 - dt.timedelta(days=300), "valid_to": OPEN_END, "source_updated_at": created,
    })
    calendar = settlement_calendar(d1 - dt.timedelta(days=1), d1 + dt.timedelta(days=30))

    # ---- day 1 readings and control
    cal1 = calendar[calendar["settlement_date"] == d1].reset_index(drop=True)
    rd1, defects1 = _inject_defects(rng, _make_readings(rng, meter["meter_id"].tolist(), cal1), cfg.defect_rate)
    final1 = _final_rows(rd1, set(meter["meter_id"]))
    day1_load_ts = utc(d1 + dt.timedelta(days=1), 2)
    ctl1 = _control(d1, int(final1["kwh_m"].sum()), len(final1), f"RUN-{d1}", 1, utc(d1 + dt.timedelta(days=1), 1))

    day1 = {
        "settlement_calendar": calendar, "tariff": tariff, "supply_point": supply_point,
        "customer": customer, "account": account, "meter": meter, "meter_assignment": assignment,
        "meter_reading": _readings_frame(rd1),
        "reading_correction": pd.DataFrame(columns=[
            "correction_id", "reading_id", "meter_id", "reading_ts", "version", "replacement_kwh", "reason",
            "corrected_at", "source_updated_at"]),
        "reconciliation_control": pd.DataFrame([ctl1]),
    }

    # ---- day 2 change set
    t_change = utc(d2, 8)
    c_change = utc(d2, 11)

    # tariff change: close the old assignment, open a new one with a different tariff
    n_tc = max(1, int(n_m * cfg.tariff_change_rate))
    tc = assignment.sample(n_tc, random_state=cfg.seed).copy()
    closed = tc.copy()
    closed["valid_to"] = d2
    closed["source_updated_at"] = t_change
    opened = tc.copy()
    opened["assignment_id"] = [f"ASG-N{i:05d}" for i in range(n_tc)]
    opened["tariff_id"] = ["TRF-GRN-04" if t != "TRF-GRN-04" else "TRF-STD-01" for t in tc["tariff_id"]]
    opened["valid_from"] = d2
    opened["valid_to"] = OPEN_END
    opened["source_updated_at"] = t_change

    # customer region change (effective mid-day)
    n_cc = max(1, int(n_c * cfg.customer_change_rate))
    cc = customer.sample(n_cc, random_state=cfg.seed + 1).copy()
    cc["region"] = [REGIONS[(REGIONS.index(r) + 1) % len(REGIONS)] for r in cc["region"]]
    cc["source_updated_at"] = c_change

    # new meters on existing accounts
    n_nm = max(1, int(n_m * cfg.new_meter_rate))
    new_ids = [f"MTR-{n_m + i:06d}" for i in range(n_nm)]
    new_sp = pd.DataFrame({
        "supply_point_id": [f"SP-{n_m + i:06d}" for i in range(n_nm)],
        "supply_ref": [f"SR{n_m + i:06d}" for i in range(n_nm)],
        "region": rng.choice(REGIONS, n_nm).tolist(), "grid_zone": "GZ1",
        "supply_type": "ELECTRICITY", "status": "ACTIVE", "source_updated_at": utc(d2, 6),
    })
    new_meter = pd.DataFrame({
        "meter_id": new_ids, "supply_point_id": new_sp["supply_point_id"],
        "serial_no": [f"SN{n_m + i:08d}" for i in range(n_nm)], "meter_type": "SMART", "status": "ACTIVE",
        "installed_at": d2, "source_updated_at": utc(d2, 6),
    })
    owners = rng.integers(0, n_c, n_nm)
    new_asg = pd.DataFrame({
        "assignment_id": [f"ASG-M{i:05d}" for i in range(n_nm)], "meter_id": new_ids,
        "account_id": [account["account_id"][o] for o in owners], "supply_point_id": new_sp["supply_point_id"],
        "tariff_id": [tariff_for(cust_type[o]) for o in owners], "valid_from": d2, "valid_to": OPEN_END,
        "source_updated_at": utc(d2, 6),
    })

    # meter status change
    faulty = meter.sample(max(1, n_m // 100), random_state=cfg.seed + 2).copy()
    faulty["status"] = "FAULTY"
    faulty["source_updated_at"] = utc(d2, 7)

    # day 2 readings
    all_meters = meter["meter_id"].tolist() + new_ids
    cal2 = calendar[calendar["settlement_date"] == d2].reset_index(drop=True)
    rd2, defects2 = _inject_defects(rng, _make_readings(rng, all_meters, cal2), cfg.defect_rate)
    final2 = _final_rows(rd2, set(all_meters))

    # corrections to day-1 readings (version 1 only, so a replay cannot race them)
    n_cor = max(1, int(len(final1) * cfg.correction_rate))
    cand = final1[final1["version"] == 1]
    picked = cand.sample(n_cor, random_state=cfg.seed + 3).copy()
    new_milli = (picked["kwh_m"] + np.rint(picked["kwh_m"] * 0.15) + 1).astype(float)
    corr_at = utc(d2, 9)
    corrections = pd.DataFrame({
        "correction_id": [f"COR-{i:06d}" for i in range(n_cor)],
        "reading_id": picked["reading_id"].tolist(), "meter_id": picked["meter_id"].tolist(),
        "reading_ts": picked["reading_ts"].tolist(), "version": 1,
        "replacement_kwh": [dec(v) for v in new_milli], "reason": "METER_FAULT",
        "corrected_at": corr_at, "source_updated_at": corr_at,
    })
    final1 = final1.set_index("reading_id")
    final1.loc[picked["reading_id"].tolist(), "kwh_m"] = new_milli.tolist()
    final1 = final1.reset_index()

    run2 = f"RUN-{d2}"
    created2 = utc(d2 + dt.timedelta(days=1), 1)
    controls2 = pd.DataFrame([
        _control(d1, int(final1["kwh_m"].sum()), len(final1), run2, 2, created2),
        _control(d2, int(final2["kwh_m"].sum()), len(final2), run2, 1, created2, inflate=cfg.bad_control),
    ])

    day2 = {
        "supply_point": _with_op(new_sp, "I"),
        "customer": _with_op(cc, "U"),
        "meter": pd.concat([_with_op(new_meter, "I"), _with_op(faulty, "U")], ignore_index=True),
        "meter_assignment": pd.concat(
            [_with_op(closed, "U"), _with_op(opened, "I"), _with_op(new_asg, "I")], ignore_index=True),
        "meter_reading": _with_op(_readings_frame(rd2), "I"),
        "reading_correction": _with_op(corrections, "I"),
        "reconciliation_control": _with_op(controls2, "I"),
    }

    expected = {
        "seed": cfg.seed,
        "day1": {"settlement_date": str(d1), "periods": len(cal1), "reading_rows": len(rd1),
                 "defects": defects1},
        "day2": {"settlement_date": str(d2), "periods": len(cal2), "reading_rows": len(rd2),
                 "defects": defects2, "corrections": n_cor, "tariff_changes": n_tc,
                 "customer_changes": n_cc, "new_meters": n_nm, "faulty_meters": len(faulty)},
        "controls": [
            {k: str(v) for k, v in row.items()} for row in [ctl1, *controls2.to_dict("records")]
        ],
        "final_totals_kwh": {
            str(d1): str(dec(int(final1["kwh_m"].sum()))), str(d2): str(dec(int(final2["kwh_m"].sum()))),
        },
        "final_counts": {str(d1): len(final1), str(d2): len(final2)},
    }
    return Scenario(cfg, day1, day2, day1_load_ts, utc(d2 + dt.timedelta(days=1), 2), expected)
