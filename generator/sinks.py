"""Where a scenario goes: the real source database (PostgreSQL) or a local imitation of DMS output."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from lakehouse.tables import LOAD_ORDER, TABLES

# Extra columns only the schema-change scenarios add (see apply_scenario).
EXTRA_FAMILIES = {"firmware_version": "string", "consumption_wh": "int"}
SCENARIOS = ("baseline", "additive", "breaking_unit")


def py(v):
    """pandas / numpy scalar -> plain Python value, missing -> None."""
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, float) and v != v:
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


# --------------------------------------------------------------------------- PostgreSQL
class PostgresSink:
    """Loads day 1 with COPY and applies day 2 as ordinary DML so DMS sees real CDC events."""

    def __init__(self, dsn: str):
        import psycopg  # imported lazily: only needed for this sink

        self.conn = psycopg.connect(dsn, autocommit=False)

    def create_schema(self, ddl_path: str | Path, reset: bool = False) -> None:
        with self.conn.cursor() as cur:
            if reset:
                cur.execute("DROP SCHEMA IF EXISTS src CASCADE")
            cur.execute(Path(ddl_path).read_text())
        self.conn.commit()

    def load_day1(self, frames: dict[str, pd.DataFrame]) -> None:
        with self.conn.cursor() as cur:
            for name in LOAD_ORDER:
                df = frames.get(name)
                if df is None or df.empty:
                    continue
                cols = list(TABLES[name].columns)
                with cur.copy(f"COPY src.{name} ({', '.join(cols)}) FROM STDIN") as cp:
                    for row in df[cols].itertuples(index=False):
                        cp.write_row([py(v) for v in row])
        self.conn.commit()

    def apply_day2(self, frames: dict[str, pd.DataFrame]) -> None:
        with self.conn.cursor() as cur:
            for name in LOAD_ORDER:
                df = frames.get(name)
                if df is None or df.empty:
                    continue
                t = TABLES[name]
                cols = list(t.columns)
                sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in t.pk)
                upsert = (f"INSERT INTO src.{name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                          f"ON CONFLICT ({', '.join(t.pk)}) DO UPDATE SET {sets}")
                delete = f"DELETE FROM src.{name} WHERE " + " AND ".join(f"{c} = %s" for c in t.pk)
                for rec in df.to_dict("records"):
                    if rec["Op"] == "D":
                        cur.execute(delete, [py(rec[c]) for c in t.pk])
                    else:
                        cur.execute(upsert, [py(rec[c]) for c in cols])
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


# --------------------------------------------------------------------------- DMS imitation
def _arrow_type(family: str):
    import pyarrow as pa

    return {
        "string": pa.string(), "int": pa.int32(), "decimal": pa.decimal128(18, 3), "double": pa.float64(),
        "timestamp": pa.timestamp("us"), "date": pa.date32(), "bool": pa.bool_(),
    }[family]


def _commit_ts(df: pd.DataFrame, fallback: dt.datetime) -> list[dt.datetime]:
    """CDC commit time: the row's own change time plus two seconds."""
    for col in ("source_updated_at", "created_at"):
        if col in df.columns:
            return [py(v) + dt.timedelta(seconds=2) for v in df[col]]
    return [fallback] * len(df)


def apply_scenario(table: str, df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    """Schema-change scenarios for the contract gate. Only reachable through the DMS imitation:
    DMS does not replicate DDL from PostgreSQL, so a real run needs a table reload for these."""
    df = df.copy()
    if scenario == "additive" and table == "meter":
        df["firmware_version"] = "v2.1"
    if scenario == "breaking_unit" and table == "meter_reading":
        df["consumption_wh"] = [None if v is None else int(v * 1000) for v in df["consumption_kwh"]]
        df = df.drop(columns=["consumption_kwh"])
    return df


def _write_parquet(table: str, df: pd.DataFrame, path: Path, commit_ts: list[dt.datetime]) -> None:
    """Families are resolved per table, never globally: the same column name can carry different
    types in different tables (`period` is an int in settlement_calendar, a date in
    reconciliation_control)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    families = TABLES[table].columns
    arrays, names = [], []
    names.append("Op")
    arrays.append(pa.array(df["Op"].tolist(), type=pa.string()))
    for col in df.columns:
        if col == "Op":
            continue
        fam = families.get(col) or EXTRA_FAMILIES[col]
        names.append(col)
        arrays.append(pa.array([py(v) for v in df[col]], type=_arrow_type(fam)))
    names.append("dms_commit_ts")
    arrays.append(pa.array(commit_ts, type=pa.timestamp("us")))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_arrays(arrays, names=names), path)


class DmsImitationSink:
    """Writes what DMS (full-load-and-cdc, parquet, Op column) would write under <root>/<table>/."""

    def __init__(self, root: str | Path, scenario: str = "baseline"):
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario!r}")
        self.root = Path(root)
        self.scenario = scenario

    def write_day1(self, frames: dict[str, pd.DataFrame], load_ts: dt.datetime) -> list[Path]:
        out = []
        for name in LOAD_ORDER:
            df = frames.get(name)
            if df is None or df.empty:
                continue
            df = df.copy()
            df.insert(0, "Op", "I")
            path = self.root / name / "LOAD00000001.parquet"
            _write_parquet(name, df, path, [load_ts] * len(df))
            out.append(path)
        return out

    def write_day2(self, frames: dict[str, pd.DataFrame], load_ts: dt.datetime) -> list[Path]:
        out = []
        for name in LOAD_ORDER:
            df = frames.get(name)
            if df is None or df.empty:
                continue
            ts = _commit_ts(df, load_ts)
            df = apply_scenario(name, df, self.scenario)
            path = self.root / name / f"{load_ts:%Y/%m/%d}" / f"{load_ts:%Y%m%d-%H%M%S}000.parquet"
            _write_parquet(name, df, path, ts)
            out.append(path)
        return out


def write_expected(expected: dict, path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(expected, indent=2, default=str))
