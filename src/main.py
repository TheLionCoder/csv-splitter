# *-* coding: utf-8 *-*
import concurrent.futures
from pathlib import Path
from typing import List

import polars as pl
from loguru import logger

from src.file_reading import load_data
from src.file_pipeline import extract_unique_categories
from src.file_writing import make_subdir, save_file


def write_file(
    query: pl.LazyFrame,
    *,
    input_column: str,
    output_format: str,
    output_dir: Path,
    make_dir: bool,
) -> None:
    """
    Write the file to the output directory
    :param query: Polars LazyFrame
    :param input_column: Column to extract unique categories from
    :param output_format: File format to save
    :param output_dir: Directory to save the file
    :param make_dir: Whether to create a directory
    """
    categories_list: List[str] = extract_unique_categories(
        query, input_col=input_column
    )
