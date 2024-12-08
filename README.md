# CSV Splitter

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://docs.astral.sh/uv/)

This project is designed to split a large csv or txt dataset into multiple
smaller files based on a specified column value.

> [!NOTE]
> Null values are keep it as `unknown`

## Installation

1.Clone the repository:

```sh
git clone git@github.com:TheLionCoder/csv-splitter.git

cd csv-splitter
```

3.Sync the project's dependencies with the environment.

> [!WARNING]
> Applies to UV only

```sh
uv sync
```

## Usage

To run the program, use the following command

_Get Help_:

```sh
uv run python src/main.y --help
```

### _Arguments_

- `input column Column to extract unique categories from`
- `--path -p Path to the file`
- `--output dir -o Output directory to save the files`
- `--make-dir -m Whether tto create directories to save each category`
- `--keep-delimiter, --keep-delim -k Whether keep the input file delimiterin
in the output.`
- `--output-format, --file-format -f either csv or txt`
- `delimiter -d Separator for input files [',', '|'', '\\t']`

### Example

To split a csv file comma separated on my root project

```sh
uv run python "State" --path ./assets/city.csv -o ./assets/data/ -m
--keep-delimiter --output-format txt
```

_The result will be like:_

```sh
assets/
├── city.csv
├── data
│   ├── AK.csv
│   ├── AL.csv
│   ├── CA.csv
│   └── NY.csv
```
