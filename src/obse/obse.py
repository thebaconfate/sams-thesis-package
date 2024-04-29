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


def load_csv(filename: str) -> pl.LazyFrame:
    """reads data from a csv file and returns a DataFrame object"""
    return common.load_csv(IN_DIR, filename)


def save_csv(lf: pl.LazyFrame, filename: str) -> None:
    """saves a DataFrame object to a csv file"""
    common.save_csv(lf, OUT_DIR, filename)


def clean_lf(
    lf: pl.LazyFrame, headers: list[str], tz_rt_ts_headers_allowed: bool = True
) -> pl.LazyFrame:
    """Deletes headers from a LazyFrame object and selects only the records where ACTIVITEIT is equal to 1"""
    if tz_rt_ts_headers_allowed:
        return lf.drop(headers).filter(pl.col("ACTIVITEIT") == 1)
    else:
        return lf.drop(headers + common.tz_rt_ts_headers(lf)).filter(
            pl.col("ACTIVITEIT") == 1
        )


def select_valid_participants(lf: pl.LazyFrame) -> list[str]:
    """Selects all participants that have at least 5 records returns a list of the participant_ids"""
    return (
        lf.group_by("PARTICIPANT_ID")
        .len()
        .filter(pl.col("len") >= 5)
        .select(pl.col("PARTICIPANT_ID"))
        .collect()
        .to_dict(as_series=False)["PARTICIPANT_ID"]
    )


def select_by_ids(lf: pl.LazyFrame, participant_ids: list[str]) -> pl.LazyFrame:
    """Selects all records where the PARTICIPANT_ID is equal to the fiven participant_id"""
    return lf.filter(pl.col("PARTICIPANT_ID").is_in(participant_ids))


def add_mean(lf: pl.LazyFrame, headers: list[str]) -> pl.LazyFrame:
    """Adds a new column to the DataFrame containing the mean of the columns in headers"""
    keep = [i for i in lf.columns if i not in headers]
    lf = lf.select(keep + [pl.col(i).cast(pl.UInt64) for i in headers])
    return lf.with_columns(mean=(sum([pl.col(i) for i in headers]) / len(headers)))


def process_obse(headers: list[str]):
    for filename in IN_DIR.iterdir():
        lf = load_csv(filename.name)
        lf = clean_lf(lf, headers, False)
        participant_ids = select_valid_participants(lf)
        lf = select_by_ids(lf, participant_ids)
        lf = add_mean(lf, ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
        save_csv(lf, filename.name)
    print("Files processed successfully!")
