# *-* coding: utf-8 *-*
import concurrent.futures
import sys
from pathlib import Path
from typing import List, Set

import polars as pl
import typer
from loguru import logger
from typing_extensions import Annotated
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1].as_posix()
logger.opt(colors=True).debug(f"Python version: {sys.version} on {sys.platform}")
sys.path.append(PROJECT_ROOT)


from src.config import FileExtension, Delimiter  # noqa: E402
from src.file_reading import load_data  # noqa: E402
from src.file_pipeline import (  # noqa: E402
    extract_unique_categories,
    has_column,
    has_null_value,
    fill_null_value,
)
from src.file_writing import prepare_and_store_file  # noqa: E402

app = typer.Typer()


@app.command()
def main(
    input_column: Annotated[
        str, typer.Argument(help="Column to extract unique categories from")
    ],
    path: Annotated[
        Path,
        typer.Option(
            "--path",
            "-p",
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            resolve_path=True,
            help="File path to process",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "-o",
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
            help="Output directory to save the files",
        ),
    ],
    make_dir: Annotated[
        bool,
        typer.Option(
            "--make-dir",
            "-m",
            is_flag=True,
            help="Whether to create directories to save each category",
        ),
    ] = False,
    keep_delimiter: Annotated[
        bool,
        typer.Option(
            "--keep-delimiter",
            "--keep-delim",
            "-k",
            is_flag=True,
            help="Whether keep the input file delimiter in the output. "
            "Default output Delimiter is '|'",
        ),
    ] = False,
    output_format: Annotated[
        FileExtension,
        typer.Option("--output-format", "--file-format", "-f", case_sensitive=False),
    ] = FileExtension.CSV,
    delimiter: Annotated[
        Delimiter,
        typer.Option(
            "--delimiter",
            "-d",
            case_sensitive=False,
            help="Separator for input files [',', '|', '\\t']",
        ),
    ] = Delimiter.COMMA,
) -> None:
    """Main function to pipeline the file"""
    if is_valid(path):
        try:
            logger.opt(colors=True).info(f"Processing file: {path}")
            process_pipeline(
                path,
                delimiter=delimiter.value,
                input_column=input_column,
                output_format=output_format.value,
                output_dir=output_dir,
                make_dir=make_dir,
                keep_delimiter=keep_delimiter,
            )
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")


def is_valid(file_path: Path) -> bool:
    """Check if the file extension is valid"""
    valid_extension: Set[str] = {FileExtension.CSV.value, FileExtension.TXT.value}
    return file_path.suffix.lower().lstrip(".") in valid_extension


def process_pipeline(
    input_path: Path,
    *,
    delimiter: str,
    input_column: str,
    output_format: str,
    output_dir: Path,
    make_dir: bool,
    keep_delimiter: bool,
) -> None:
    """Process the pipeline"""
    file_name: str = input_path.stem
    output_delimiter: str = delimiter if keep_delimiter else Delimiter.PIPE.value

    query: pl.LazyFrame = load_data(input_path, delimiter)
    if not has_column(query, input_column):
        return
    if has_null_value(query, input_column):
        logger.opt(colors=True).warning(
            "Null values found in the column, will be categorized as "
            f"{input_column}_null"
        )
        query = fill_null_value(query, input_column)

    write_file(
        query,
        input_column=input_column,
        output_format=output_format,
        delimiter=output_delimiter,
        file_name=file_name,
        output_dir=output_dir,
        make_dir=make_dir,
    )


def write_file(
    query: pl.LazyFrame,
    *,
    input_column: str,
    output_format: str,
    delimiter: str,
    file_name: str,
    output_dir: Path,
    make_dir: bool,
) -> None:
    """
    Write the file to the output directory
    :param query: Polars LazyFrame
    :param input_column: Column to extract unique categories from
    :param output_format: File format to save
    :param delimiter: Separator to save the file.
    Default is "|"
    :param file_name: the Name of the file
    :param output_dir: Directory to save the file
    :param make_dir: Whether to create a directory
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
                delimiter,
                make_dir,
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


if __name__ == "__main__":
    app()
