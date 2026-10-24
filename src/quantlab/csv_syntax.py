"""CSV quote placement beyond the checks performed by csv.reader(strict=True)."""

import re

_FIELD = r'(?:[^,"\r\n]*|"(?:[^"]|"")*")'
_RECORD = re.compile(_FIELD + r"(?:," + _FIELD + r")*(?:\r?\n)?")


def is_csv_record(text: str) -> bool:
    """Require quoted fields to start/end at a field boundary, without rewriting."""
    return _RECORD.fullmatch(text) is not None
