# -*- coding: utf-18 -*-
from pathlib import Path

import polars as pl

from src.config import FileExtension


def load_data(path: Path, delimiter: str) -> pl.LazyFrame:
    """
    Load data from a file.
    :param path: File a path
    :param delimiter: delimiter used in the file
    """
    suffix: str = path.suffix.lstrip(".")
    if suffix == FileExtension.PARQUET.value:
        return pl.scan_parquet(path)
    return pl.scan_csv(path, separator=delimiter, infer_schema=False)
