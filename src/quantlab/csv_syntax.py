"""Bounded CSV record framing and quote checks beyond csv.reader(strict=True)."""

import re
from typing import BinaryIO

_FIELD = r'(?:[^,"\r\n]*|"(?:[^"]|"")*")'
_RECORD = re.compile(_FIELD + r"(?:," + _FIELD + r")*(?:\r?\n)?")


def is_csv_record(text: str) -> bool:
    """Require quoted fields to start/end at a field boundary, without rewriting."""
    return _RECORD.fullmatch(text) is not None


class CSVRecordLines:
    """Feed csv.reader physical lines while retaining bounded original bytes."""

    def __init__(self, stream: BinaryIO, max_record_bytes: int) -> None:
        self.stream = stream
        self.limit = max_record_bytes
        self.line_number = 0
        self.byte_offset = 0
        self.record = bytearray()

    def __iter__(self) -> "CSVRecordLines":
        return self

    def __next__(self) -> str:
        remaining = self.limit - len(self.record)
        line = self.stream.readline(remaining + 1)
        if not line:
            raise StopIteration
        if len(line) > remaining:
            raise OverflowError("CSV record exceeds byte limit")
        # LF/CRLF are supported; reject bare CR rather than normalizing evidence.
        if b"\x00" in line:
            raise ValueError("NUL is not supported in CSV text")
        if b"\r" in line.replace(b"\r\n", b""):
            raise ValueError("CSV requires LF or CRLF line endings")
        self.record.extend(line)
        self.line_number += 1
        self.byte_offset += len(line)
        return line.decode("utf-8")
