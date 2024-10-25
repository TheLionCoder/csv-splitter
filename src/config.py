from enum import Enum


class FileExtension(str, Enum):
    CSV = "csv"
    TXT = "txt"


class Delimiter(str, Enum):
    COMMA = ","
    TAB = "\t"
    PIPE = "|"
    SEMICOLON = ";"
