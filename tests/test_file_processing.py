# -*_ encoding: utf-8 -*-
import sys
from typing import List

from polars import LazyFrame
from polars.testing import assert_series_equal

sys.path.extend([".", ".."])

import polars as pl

from src.file_pipeline import (
    extract_unique_categories,
    has_column,
    has_null_value,
    fill_null_value,
)


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
    assert has_column(query, input_column="foo")


def test_has_column_not_found():
    query: pl.LazyFrame = make_dataframe()
    assert not has_column(query, "baz")


def test_has_null_value():
    query: pl.LazyFrame = make_dataframe()
    assert has_null_value(query, "spam")


def test_has_no_null_value():
    query: pl.LazyFrame = make_dataframe()
    assert not has_null_value(query, "bar")


def test_fill_null_value():
    query: pl.LazyFrame = make_dataframe()
    df: pl.DataFrame = fill_null_value(query, "spam").collect()
    assert_series_equal(
        pl.Series("spam", ["a", "b", "spam_null", "spam_null"]), df.get_column("spam")
    )
