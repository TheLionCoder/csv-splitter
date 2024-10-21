# -*- coding: utf-8 -*-
from pathlib import Path

import polars as pl
from xlsxwriter import Workbook


def prepare_and_store_file(
    query: pl.LazyFrame,
    input_column: str, 
    category_value: str,
    dir_path: Path,
    file_name: str,
    file_extension: str,
    delimiter: str,
    make_dir: bool,
) -> None:
    """
    Prepare and store the file
    :param query: LazyFrame to store
    :param input_column: Column to extract unique categories from
    :param input_column: Column to extract unique categories from
    :param category_value: Category value to store
    :param dir_path: Path to store the file
    :param file_name: Name of the file
    :param file_extension:  extension to store
    :param delimiter: Separator to store the file. If not, plain text [csv, txt] is ignored.
    :param make_dir: Whether to create a directory
    """
    query: pl.LazyFrame = (query.filter(pl.col(input_column).eq(pl.lit(category_value)))
                           .select(pl.all().exclude([input_column])))
    file_name = f"{file_name}.{file_extension}"
    if make_dir:
        make_subdir(dir_path, category_value)
        file_path: Path = dir_path.joinpath(category_value, file_name)
    else:
        file_path: Path = dir_path.joinpath(file_name).with_stem(category_value)

    save_file_as_csv(query, file_path=file_path, separator=delimiter)



def save_file_as_csv(query: pl.LazyFrame, *, file_path: Path, separator: str) -> None:
    """
    Save the file as csv
    """
    query.sink_csv(file_path, separator=separator)


# Todo: Deprecated
def save_file_as_xlsx(query: pl.LazyFrame, file_path: Path) -> None:
    """
    Save the file as xlsx
    """
    df: pl.DataFrame = query.collect()
    with Workbook(file_path) as wb:
        df.write_excel(wb, worksheet="table", autofit=True)


def make_subdir(dir_path: Path, dir_name: str) -> None:
    """
    Create the directory.
    """
    dir_path.joinpath(dir_name).mkdir(parents=True, exist_ok=True)
