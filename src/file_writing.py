# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Optional

import polars as pl

from src.config import Delimiter, FileExtension


def prepare_and_store_file(
    query: pl.LazyFrame,
    input_column: str,
    category_value: str,
    dir_path: Path,
    file_name: str,
    file_extension: FileExtension,
    create_dir: bool,
) -> None:
    """
    Prepare and store the file
    :param query: LazyFrame to store
    :param input_column: Column to extract unique categories from
    :param category_value: Category value to store
    :param dir_path: Path to store the file
    :param file_name: Name of the file
    :param file_extension:  extension to store
    :param create_dir: Whether to create a directory
    """
    lf: pl.LazyFrame = query.filter(
        pl.col(input_column).eq(pl.lit(category_value))
    ).select(pl.all().exclude([input_column]))

    file_name = f"{file_name}.{file_extension.value}"
    file_path: Optional[Path] = create_category_path(
        dir_path, category_value, create_dir, file_name
    )
    if file_path:
        lf.sink_csv(file_path, separator=Delimiter.PIPE.value)


def create_category_path(
    dir_path: Path, category_value: str, create_dir: bool, file_name: str
) -> Optional[Path]:
    """
    Create a category path
    """
    if category_value in ["..", "/", "\\"]:
        return None

    category_dir: Path = dir_path.joinpath(category_value)

    if create_dir and not category_dir.exists():
        category_dir.mkdir(parents=True, exist_ok=True)
        file_path: Path = category_dir.joinpath(category_value, file_name)
    else:
        file_path: Path = dir_path.joinpath(file_name).with_stem(category_value)

    return file_path
