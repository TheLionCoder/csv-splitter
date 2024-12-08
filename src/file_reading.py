# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Set

import polars as pl

from src.config import Delimiter, FileExtension


def load_data(path: Path, delimiter: Delimiter, col: pl.Expr) -> pl.LazyFrame:
    """
    Load data from a file.
    :param path: File a path
    :param delimiter: delimiter used in the file
    :param col: Column to fill null values
    """
    return pl.scan_csv(
        path, separator=delimiter.value, infer_schema=False
    ).with_columns([col.fill_null(pl.lit("unknown"))])


def has_column(schema: pl.schema, input_column: str) -> bool:
    """
    Check if a column name exists in the dataframe
    :param schema: Schema of the file
    :param input_column: Column to check
    :return: Boolean value
    """
    return input_column in schema.names()


def is_valid(file_path: Path) -> bool:
    """Check if the file extension is valid"""
    valid_extension: Set[str] = {FileExtension.CSV.value, FileExtension.TXT.value}
    return file_path.suffix.lower().lstrip(".") in valid_extension
