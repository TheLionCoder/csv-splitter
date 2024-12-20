# *-* coding: utf-8 *-*
import sys
from pathlib import Path

import typer
from loguru import logger
from typing_extensions import Annotated

PROJECT_ROOT = Path(__file__).resolve().parents[1].as_posix()
logger.opt(colors=True).debug(f"Python version: {sys.version} on {sys.platform}")
sys.path.append(PROJECT_ROOT)


from src.config import FileExtension, Delimiter  # noqa: E402
from src.file_reading import is_valid  # noqa: E402
from src.file_processing import process_pipeline  # noqa: E402

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
    create_dir: Annotated[
        bool,
        typer.Option(
            "--create-dir",
            "-c",
            is_flag=True,
            help="Whether to create directories to save each category",
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
                delimiter=delimiter,
                input_column=input_column,
                output_format=output_format,
                output_dir=output_dir,
                create_dir=create_dir,
            )
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")


if __name__ == "__main__":
    app()
