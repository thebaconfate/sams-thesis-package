from pathlib import Path
import polars as pl
from common import common

OUT_DIR = Path("../resources/out/afvar")
if OUT_DIR.parent.parent.exists() is False:
    OUT_DIR.parent.parent.mkdir()
if OUT_DIR.parent.exists() is False:
    OUT_DIR.parent.mkdir()
if OUT_DIR.exists() is False:
    OUT_DIR.mkdir()


def save_csv(lf: pl.LazyFrame, filename: str) -> None:
    """saves a LazyFrame object to a csv file"""
    common.save_csv(lf, OUT_DIR, filename)


def save_excel(lf: pl.LazyFrame, filename: str):
    """Saves a LazyFrame object to an excel file"""
    common.save_excel(lf, OUT_DIR, filename)


def clean_lf(
    lf: pl.LazyFrame, headers: list[str], tz_rt_ts_headers_allowed: bool = True
) -> pl.LazyFrame:
    """Deletes headers from a LazyFrame object"""
    if tz_rt_ts_headers_allowed:
        return lf.drop(headers)
    else:
        return lf.drop(headers + common.tz_rt_ts_headers(lf))


def process_afvar(
    lf: pl.LazyFrame, headers: list[str], tz_rt_ts_headers_allowed: bool = True
):
    cleaned_lf = clean_lf(lf, headers, tz_rt_ts_headers_allowed)
    return cleaned_lf
