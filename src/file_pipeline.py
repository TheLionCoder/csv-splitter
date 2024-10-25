# *-* encoding: utf-8 *-*
from typing import List

import polars as pl


def extract_unique_categories(query: pl.LazyFrame, *, input_col: str) -> List[str]:
    """
    Extract unique categories from the dataframe.
    :param query: LazyFrame to extract unique categories from
    :param input_col: Column to extract unique categories from
    :return: List of unique categories
    """
    column: pl.Expr = pl.col(input_col)
    return query.select([column]).unique().collect().get_column(input_col).to_list()


def has_column(query: pl.LazyFrame, input_column: str) -> bool:
    """
    Check if a column name exists in the dataframe
    :param query: Polars LazyFrame
    :param input_column: Column to check
    :return: Boolean value
    """
    return input_column in query.collect_schema().names()


def has_null_value(query: pl.LazyFrame, input_column: str) -> bool:
    """
    Check if a column has null values
    :param query: LazyFrame
    :param input_column: Column to check
    :return: Boolean value
    """
    count: int = query.select(pl.col(input_column).null_count()).collect().item()
    return count > 0


def fill_null_value(query: pl.LazyFrame, input_column: str) -> pl.LazyFrame:
    """
    Fill null values
    :param query: LazyFrame
    :param input_column: Column to fill null values
    :return pl.LazyFrame
    """
    value: pl.Expr = pl.lit("unknown")
    return query.with_columns([pl.col(input_column).fill_null(value)])
