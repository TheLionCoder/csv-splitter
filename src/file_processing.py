# *-* encoding: utf-8 *-*
import concurrent.futures
from pathlib import Path
from typing import List

import polars as pl
from tqdm import tqdm

from config import Delimiter, FileExtension
from src.file_pipeline import extract_unique_categories
from src.file_reading import load_data
from file_writing import prepare_and_store_file


def process_pipeline(
    input_path: Path,
    *,
    delimiter: Delimiter,
    input_column: str,
    output_format: FileExtension,
    output_dir: Path,
    create_dir: bool,
) -> None:
    """Process the pipeline"""
    file_name: str = input_path.stem
    col: pl.Expr = pl.col(input_column)

    query: pl.LazyFrame = load_data(input_path, delimiter, col)

    write_file(
        query,
        input_column=input_column,
        output_format=output_format,
        file_name=file_name,
        output_dir=output_dir,
        create_dir=create_dir,
    )


def write_file(
    query: pl.LazyFrame,
    *,
    input_column: str,
    output_format: FileExtension,
    file_name: str,
    output_dir: Path,
    create_dir: bool,
) -> None:
    """
    Write the file to the output directory
    :param query: Polars LazyFrame
    :param input_column: Column to extract unique categories from
    :param output_format: File format to save
    :param file_name: the Name of the file
    :param output_dir: Directory to save the file
    :param create_dir: Whether to create a directory
    """
    categories_list: List[str] = extract_unique_categories(
        query, input_col=input_column
    )
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                prepare_and_store_file,
                query,
                input_column,
                category_value,
                output_dir,
                file_name,
                output_format,
                create_dir,
            )
            for category_value in categories_list
        ]
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            desc="Writing files..",
            colour="green",
            total=len(futures),
        ):
            future.result()
