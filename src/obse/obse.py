import polars as pl
from pathlib import Path
from common import common

# ! GLOBAL CONSTANTS: DO NOT TOUCH

OUT_DIR = Path("../resources/out/obse")
if OUT_DIR.parent.parent.exists() is False:
    OUT_DIR.parent.parent.mkdir()
if OUT_DIR.parent.exists() is False:
    OUT_DIR.parent.mkdir()
if OUT_DIR.exists() is False:
    OUT_DIR.mkdir()


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
    return lf.with_columns(MEAN=(sum([pl.col(i) for i in headers]) / len(headers)))


def is_obse_survey(lf: pl.LazyFrame) -> bool:
    return lf.filter(pl.col("SURVEY_NAME") == "OBSE").collect().shape[0] > 1


def process_obse(
    lf: pl.LazyFrame, headers: list[str], tz_rt_ts_headers_allowed=True
) -> pl.LazyFrame:
    lf = clean_lf(lf, headers, tz_rt_ts_headers_allowed)
    participant_ids = select_valid_participants(lf)
    lf = select_by_ids(lf, participant_ids)
    return add_mean(lf, [str(i) for i in range(1, 11)])


def create_daily_means(lf: pl.LazyFrame) -> pl.LazyFrame:
    max_days: pl.LazyFrame = (
        lf.group_by("PARTICIPANT_ID")
        .len()
        .max()
        .collect()
        .to_dict(as_series=False)["len"][0]
    )
    new_headers = ["PARTICIPANT_ID"] + ["DAY_" + str(i) for i in range(1, max_days + 1)]
    schema = {k: pl.String if k.endswith("ID") else pl.Float64 for k in new_headers}
    dicts = (
        lf.sort("COMPLETED_TS")
        .group_by("PARTICIPANT_ID", maintain_order=True)
        .agg(pl.col("MEAN").gather_every(1))
        .collect()
        .to_dicts()
    )
    records = []
    for dict in dicts:
        record = (
            [dict["PARTICIPANT_ID"]]
            + [mean for mean in dict["MEAN"]]
            + [None for _ in range(max_days - len(dict["MEAN"]))]
        )
        record = {new_headers[i]: record[i] for i in range(len(record))}
        records.append(record)
    return pl.concat([pl.DataFrame(record, schema=schema).lazy() for record in records])
