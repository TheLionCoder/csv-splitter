from enum import Enum


class FileExtension(str, Enum):
    CSV = "csv"
    TXT = "txt"
    EXCEL = "xlsx"


class Separator(str, Enum):
    COMMA = ","
    TAB = "\t"
    PIPE = "|"
