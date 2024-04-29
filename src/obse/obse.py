from ast import Str
import polars as pl
from pathlib import Path
from common import common

# ! GLOBAL CONSTANTS: DO NOT TOUCH
IN_DIR = Path("../resources/obse/data")
OUT_DIR = Path("../resources/obse/out")
if IN_DIR.parent.parent.exists() is False:
    IN_DIR.parent.parent.mkdir()
if IN_DIR.parent.exists() is False:
    IN_DIR.parent.mkdir()
if IN_DIR.exists() is False:
    IN_DIR.mkdir()
if OUT_DIR.exists() is False:
    OUT_DIR.mkdir()
print(f"{IN_DIR.exists()} {OUT_DIR.exists()}")
for filename in IN_DIR.iterdir():
    print(filename)


def load_csv(filename: str) -> pl.LazyFrame:
    """reads data from a csv file and returns a DataFrame object"""
    return common.load_csv(IN_DIR, filename)


def save_csv(lf: pl.LazyFrame, filename: str) -> None:
    """saves a DataFrame object to a csv file"""
    common.save_csv(lf, OUT_DIR, filename)

def tz_rt_ts_headers(lf : pl.LazyFrame) -> list[str]:
    """Creates a list containing all headers with _TZ, _RT or _TS in them Feel free to edit"""
    return [
        i for i in lf.columns if "_TZ" in i or "_RT" in i or "_TS" in i
    ]

def delete_headers(lf: pl.LazyFrame, headers : list[str], tz_rt_ts_headers_allowed: bool=True) -> pl.LazyFrame:
    """Deletes headers from a LazyFrame object"""
    if tz_rt_ts_headers_allowed:
        return lf.drop(headers)
    else:
        return lf.drop(headers + tz_rt_ts_headers(lf))

def select_by_activity(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Selects all the recors where ACTIVITEIT is 1"""
    return lf.filter(pl.col("ACTIVITEIT") == 1)


def select_by_name(df: DataFrame, participant_id: str) -> DataFrame:
    """Selects all records where the PARTICIPANT_ID is equal to the fiven participant_id"""
    return df.loc[df["PARTICIPANT_ID"] == participant_id]
