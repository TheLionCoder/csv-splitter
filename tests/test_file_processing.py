# -*_ encoding: utf-8 -*-
import sys
from typing import List

from polars import LazyFrame

sys.path.extend([".", ".."])

import polars as pl

from src.file_reading import has_column
from src.file_pipeline import (
    extract_unique_categories,
)
from src.config import FileExtension


def make_dataframe() -> pl.LazyFrame:
    query: pl.LazyFrame = LazyFrame(
        {
            "foo": [1, 2, 3, 1],
            "bar": ["a", "a", "a", "a"],
            "ham": ["x", "x", "y", "y"],
            "spam": ["a", "b", None, None],
        }
    )
    return query


def test_extract_categories():
    query: pl.LazyFrame = make_dataframe()
    categories: List[str] = extract_unique_categories(query, input_col="ham")
    assert sorted(categories) == ["x", "y"]


def test_has_column():
    query: pl.LazyFrame = make_dataframe()
    schema = query.collect_schema()
    assert has_column(schema, input_column="foo")


def test_has_column_not_found():
    query: pl.LazyFrame = make_dataframe()
    schema = query.collect_schema()
    assert not has_column(schema, "baz")


def test_file_extension():
    assert FileExtension.CSV.value == "csv"
