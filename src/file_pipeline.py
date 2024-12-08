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
