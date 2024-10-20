# CSV Splitter

This project is designed to split a large csv or txt dataset into multiple
smaller files based on a specified column value.

## Installation

1.Clone the repository:

```sh
git clone git@github.com:TheLionCoder/csv-splitter.git

cd csv-splitter
```

2.Install UV\_:
To install UV "<https://docs.astral.sh/uv/getting-started/installation/>"

3.Sync the project's dependencies with the environment <<if step 2 is already complete>>

```sh
uv sync
```

## Usage

To run the program, use the following commandi

_Get Help_:

```sh
python3 src/main.py --help
```

or uv

```sh
uv run python src/main.y --help
```

### _Arguments_

- `input column Column to extract unique categories from`
- `--path -p Path to the file or directory`
- `--output dir -o Output directory to save the files`
- `--make-dir -m Whether tto create directories to save each category`
- `--keep-delimiter, --keep-delim -k Whether keep the input file delimiter in the output.`
- `--output-format, --file-format -f either csv or txt`
- `delimiter -d Separator for input files [',', '|'', '\\t']`

### Example

To split a csv file comma separated on my root project

```sh
uv run python "State" --path ./assets/city.csv -o ./assets/data/ -m
--keep-delim --output-format txt`
```
