"""Surrogate keys. One helper, used by every dimension and every fact, so keys always agree."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

UNKNOWN_SK = -1


def _col(c) -> Column:
    return F.col(c) if isinstance(c, str) else c


def sk(*cols) -> Column:
    """Deterministic 64-bit key from the string form of the business key columns."""
    return F.xxhash64(F.concat_ws("|", *[_col(c).cast("string") for c in cols]))
