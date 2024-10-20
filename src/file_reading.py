# -*- coding: utf-8 -*-
from pathlib import Path

import polars as pl


def load_data(path: Path, delimiter: str) -> pl.LazyFrame:
    """
    Load data from a file.
    :param path: File a path
    :param delimiter: delimiter used in the file
    """
    return pl.scan_csv(path, separator=delimiter, infer_schema=False)
