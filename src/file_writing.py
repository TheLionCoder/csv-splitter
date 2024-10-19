# -*- coding: utf-8 -*-
from pathlib import Path

import polars as pl

from src.config import FileExtension


 def prepare_and_store_file(dataframe: pl.DataFrame,*,
                            category_value: str,
                            dir_path: Path,
                            file_extension: str,
                            separator: str,
                            make_dir: bool) -> None:
     """
     Prepare and store the file
     :param dataframe: Dataframe to store
     :param category_value: Category value to store
     :param dir_path: Path to store the file
     :param file_extension:  extension to store
     :param separator: Separator to store the file. If not, plain text [csv, txt] is ignored.
     :param make_dir: Whether to create a directory
     """
     ...



def save_file(dataframe: pl.DataFrame, *, file_path: Path, file_extension: str,
              separator: str) -> None:
    """
    Save the file
    """
    match file_extension:
        case FileExtension.CSV | FileExtension.TXT:
            dataframe.write_csv(file_path, separator=separator)
        case FileExtension.PARQUET:
            dataframe.write_parquet(file_path)
        case FileExtension.EXCEL:
            dataframe.write_excel(file_path, worksheet="table")
        case _:
            raise ValueError(f"Unsupported file extension: {file_extension}")


def make_subdir(dir_path: Path, dir_name: str) -> None:
    """
    Create the directory.
    """
    dir_path.joinpath(dir_name).mkdir(parents=True, exist_ok=True)


