from pathlib import Path
import polars as pl

IN_DIR = Path("../resources/in")
if IN_DIR.parent.exists() is False:
    IN_DIR.parent.mkdir()
if IN_DIR.exists() is False:
    IN_DIR.mkdir()


def load_csv(filename: str, dir: Path = IN_DIR) -> pl.LazyFrame:
    """reads data from a csv file and returns a DataFrame object"""
    return pl.read_csv(f"{dir.as_posix()}/{filename}").lazy()


def save_csv(lf: pl.LazyFrame, dir: Path, filename: str) -> None:
    """saves a LazyFrame object to a csv file"""
    if dir.exists() is False:
        dir.mkdir()
    lf.collect().write_csv(f"{dir.as_posix()}/{filename}")


def tz_rt_ts_headers(lf: pl.LazyFrame) -> list[str]:
    """Creates a list containing all headers with _TZ, _RT or _TS in them Feel free to edit"""
    return [
        i
        for i in lf.columns
        if ("_TZ" in i or "_RT" in i or "_TS" in i) and i != "COMPLETED_TS"
    ]
