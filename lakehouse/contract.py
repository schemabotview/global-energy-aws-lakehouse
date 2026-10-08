"""Schema contract gate for files landed by DMS.

Runs before Bronze (as a Glue job in AWS). For every new parquet file it compares the column families with
the contract in lakehouse/tables.py:

  same      -> file passes
  additive  -> new column(s) only: file passes, the change is recorded for review
  breaking  -> a column was dropped, or its family changed (for example kWh -> Wh as an integer):
               the file is moved to the quarantine location, the table is HELD (flag file) and every later file for
               that table is quarantined too, so Silver never mixes pre- and post-change data.
               A person clears the hold with clear_hold() after deciding what to do.

The result is a manifest listing exactly which files Bronze may load, which also closes the gap between
"checked" and "loaded" when DMS keeps writing.
"""
from __future__ import annotations

import datetime as dt
import io
from collections.abc import Callable
from dataclasses import dataclass

from lakehouse.store import Store
from lakehouse.tables import META_COLUMNS, TABLES

SAME, ADDITIVE, BREAKING = "SAME", "ADDITIVE", "BREAKING"
STATE_KEY = "contract_state.json"
FLAG_PREFIX = "quarantine_flags/"
OVERLAP = dt.timedelta(minutes=10)


@dataclass(frozen=True)
class Classification:
    kind: str
    added: tuple[str, ...] = ()
    dropped: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()


def contract_for(table: str) -> dict[str, str]:
    return {**TABLES[table].columns, **META_COLUMNS}


def classify(contract: dict[str, str], observed: dict[str, str]) -> Classification:
    added = tuple(sorted(set(observed) - set(contract)))
    dropped = tuple(sorted(set(contract) - set(observed)))
    changed = tuple(sorted(c for c in contract if c in observed and contract[c] != observed[c]))
    if dropped or changed:
        return Classification(BREAKING, added, dropped, changed)
    return Classification(ADDITIVE if added else SAME, added)


def arrow_family(t) -> str:
    import pyarrow.types as pt

    if pt.is_string(t) or pt.is_large_string(t):
        return "string"
    if pt.is_integer(t):
        return "int"
    if pt.is_decimal(t):
        return "decimal"
    if pt.is_floating(t):
        return "double"
    if pt.is_timestamp(t):
        return "timestamp"
    if pt.is_date(t):
        return "date"
    if pt.is_boolean(t):
        return "bool"
    return str(t)


def parquet_families(store: Store, key: str) -> dict[str, str]:
    """Column -> family from a parquet file in a store. Reads the whole object: fine for DMS-sized files."""
    import pyarrow.parquet as pq

    schema = pq.read_schema(io.BytesIO(store.read_bytes(key)))
    return {f.name: arrow_family(f.type) for f in schema}


def held_tables(ctl: Store) -> set[str]:
    return {o.key[len(FLAG_PREFIX):].removesuffix(".json") for o in ctl.list(FLAG_PREFIX)}


def clear_hold(ctl: Store, table: str) -> None:
    ctl.delete(f"{FLAG_PREFIX}{table}.json")


def _iso(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).isoformat()


def run_check(
    landing: Store,
    quarantine: Store,
    ctl: Store,
    run_id: str,
    schema_of: Callable[[str], dict[str, str]] | None = None,
) -> dict:
    """Check new files and write ctl/manifests/<run_id>.json. Idempotent per run_id."""
    manifest_key = f"manifests/{run_id}.json"
    existing = ctl.read_json(manifest_key)
    if existing is not None:
        return existing

    schema_of = schema_of or (lambda key: parquet_families(landing, key))
    state = ctl.read_json(STATE_KEY) or {}
    since = dt.datetime.fromisoformat(state["checked_through"]) - OVERLAP if state.get("checked_through") else None
    listed = [o for o in landing.list() if o.key.endswith(".parquet") and (since is None or o.mtime > since)]
    # The watermark is deliberately rewound by OVERLAP so a file that lands out of order is still seen. That
    # re-lists files the previous run already checked, so the keys inside the window are remembered and skipped
    # here: without this a re-listed file is passed into a second manifest and Bronze loads it twice.
    seen = set(state.get("checked_keys", []))
    files = [o for o in listed if o.key not in seen]
    files.sort(key=lambda o: (o.mtime, o.key))

    held = held_tables(ctl)
    passed: dict[str, list[str]] = {}
    quarantined: list[dict] = []
    additive: dict[str, set[str]] = {}

    for o in files:
        table = o.key.split("/")[0]
        if table not in TABLES:
            continue
        if table in held:
            quarantined.append({"table": table, "key": o.key, "reason": "TABLE_HELD"})
            continue
        result = classify(contract_for(table), schema_of(o.key))
        if result.kind == BREAKING:
            held.add(table)
            ctl.write_json(f"{FLAG_PREFIX}{table}.json", {
                "table": table, "first_bad_file": o.key, "run_id": run_id, "held_at": _iso(dt.datetime.now(dt.timezone.utc)),
                "dropped": list(result.dropped), "changed": list(result.changed), "added": list(result.added),
            })
            quarantined.append({"table": table, "key": o.key, "reason": "BREAKING_SCHEMA",
                                "dropped": list(result.dropped), "changed": list(result.changed)})
        else:
            passed.setdefault(table, []).append(o.key)
            if result.kind == ADDITIVE:
                additive.setdefault(table, set()).update(result.added)

    for q in quarantined:
        landing.copy_to(q["key"], quarantine, f"{run_id}/{q['key']}")
        landing.delete(q["key"])

    # Advances over everything listed, not just the files checked now: a file skipped as already seen still
    # belongs behind the watermark.
    checked_through = max([o.mtime for o in listed], default=None)
    if checked_through is None:
        checked_through_iso, checked_keys = state.get("checked_through"), sorted(seen)
    else:
        checked_through_iso = _iso(checked_through)
        checked_keys = sorted(o.key for o in listed if o.mtime > checked_through - OVERLAP)

    manifest = {
        "run_id": run_id,
        "created_at": _iso(dt.datetime.now(dt.timezone.utc)),
        "passed": passed,
        "quarantined": quarantined,
        "additive": [{"table": t, "added": sorted(c)} for t, c in sorted(additive.items())],
        "has_quarantine": bool(quarantined),
    }
    ctl.write_json(manifest_key, manifest)
    if checked_through_iso:
        ctl.write_json(STATE_KEY, {"checked_through": checked_through_iso, "checked_keys": checked_keys})
    return manifest
