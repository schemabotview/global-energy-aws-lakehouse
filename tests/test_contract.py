from lakehouse import contract
from lakehouse.store import LocalStore
from lakehouse.tables import TABLES


def families(table: str, **overrides):
    fam = contract.contract_for(table)
    for k, v in overrides.items():
        if v is None:
            fam.pop(k)
        else:
            fam[k] = v
    return fam


def test_classify():
    base = contract.contract_for("meter")
    assert contract.classify(base, dict(base)).kind == contract.SAME
    assert contract.classify(base, {**base, "firmware_version": "string"}).kind == contract.ADDITIVE
    dropped = {k: v for k, v in base.items() if k != "serial_no"}
    assert contract.classify(base, dropped).kind == contract.BREAKING
    assert contract.classify(base, {**base, "serial_no": "int"}).kind == contract.BREAKING


def test_contract_covers_every_table_with_dms_columns():
    for name in TABLES:
        c = contract.contract_for(name)
        assert c["Op"] == "string" and c["dms_commit_ts"] == "timestamp"


def _stores(tmp_path):
    return LocalStore(tmp_path / "landing"), LocalStore(tmp_path / "quarantine"), LocalStore(tmp_path / "ctl")


def test_breaking_change_quarantines_and_holds_table(tmp_path):
    landing, quar, ctl = _stores(tmp_path)
    schemas = {
        "meter/LOAD00000001.parquet": families("meter"),
        "meter_reading/LOAD00000001.parquet": families("meter_reading"),
        "meter_reading/2026/10/26/a.parquet": families("meter_reading", consumption_kwh=None, consumption_wh="int"),
        "meter_reading/2026/10/26/b.parquet": families("meter_reading"),
    }
    for key in schemas:
        landing.write_bytes(key, b"x")

    m = contract.run_check(landing, quar, ctl, "r1", schema_of=schemas.__getitem__)

    assert m["has_quarantine"]
    assert m["passed"] == {"meter": ["meter/LOAD00000001.parquet"],
                           "meter_reading": ["meter_reading/LOAD00000001.parquet"]}
    reasons = {q["key"]: q["reason"] for q in m["quarantined"]}
    assert reasons["meter_reading/2026/10/26/a.parquet"] == "BREAKING_SCHEMA"
    assert reasons["meter_reading/2026/10/26/b.parquet"] == "TABLE_HELD"  # later files wait behind the break
    assert contract.held_tables(ctl) == {"meter_reading"}
    assert quar.exists("r1/meter_reading/2026/10/26/a.parquet")
    assert not landing.exists("meter_reading/2026/10/26/a.parquet")

    contract.clear_hold(ctl, "meter_reading")
    assert contract.held_tables(ctl) == set()


def test_additive_change_passes_and_is_recorded(tmp_path):
    landing, quar, ctl = _stores(tmp_path)
    landing.write_bytes("meter/x.parquet", b"x")
    fam = {**families("meter"), "firmware_version": "string"}
    m = contract.run_check(landing, quar, ctl, "r1", schema_of=lambda k: fam)
    assert not m["has_quarantine"]
    assert m["passed"] == {"meter": ["meter/x.parquet"]}
    assert m["additive"] == [{"table": "meter", "added": ["firmware_version"]}]


def test_run_check_is_idempotent_per_run_id(tmp_path):
    landing, quar, ctl = _stores(tmp_path)
    landing.write_bytes("meter/x.parquet", b"x")
    first = contract.run_check(landing, quar, ctl, "r1", schema_of=lambda k: families("meter"))
    again = contract.run_check(landing, quar, ctl, "r1", schema_of=lambda k: families("meter"))
    assert first == again and again["passed"] == {"meter": ["meter/x.parquet"]}
