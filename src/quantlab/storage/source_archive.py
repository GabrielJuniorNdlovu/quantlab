"""Bounded source copying and no-overwrite publication of verified original bytes."""

import hashlib
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, BinaryIO

from quantlab.storage.contracts import ArchivedSource, StorageError

_CHUNK_BYTES = 1024 * 1024


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _digest(stream: IO[bytes]) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    while chunk := stream.read(_CHUNK_BYTES):
        total += len(chunk)
        digest.update(chunk)
    return digest.hexdigest(), total


def _open_regular(path: Path) -> BinaryIO:
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise StorageError("source must be a regular file")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


class SourceArchive:
    """Content-addressed files prevent accidental overwrite, not malicious editing.

    Incomplete copies retain a `.partial-` name for diagnosis and never register
    as an archived source. Each hash/copy pass uses at most a 1 MiB input chunk.
    """

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def preserve(self, path: Path) -> ArchivedSource:
        self.root.mkdir(parents=True, exist_ok=True)
        with _open_regular(path) as original:
            before = os.fstat(original.fileno())
            with tempfile.NamedTemporaryFile(
                mode="w+b", prefix=".partial-", dir=self.root, delete=False
            ) as copy:
                partial = Path(copy.name)
                digest = hashlib.sha256()
                count = 0
                while chunk := original.read(_CHUNK_BYTES):
                    copy.write(chunk)
                    digest.update(chunk)
                    count += len(chunk)
                copy.flush()
                os.fchmod(copy.fileno(), 0o444)
                os.fsync(copy.fileno())
                if _identity(before) != _identity(os.fstat(original.fileno())) or _identity(
                    before
                ) != _identity(path.stat()):
                    raise StorageError("source changed during archival")
                copy.seek(0)
                if _digest(copy.file) != (digest.hexdigest(), count):
                    raise StorageError("copied source checksum mismatch")
            source = ArchivedSource(
                digest.hexdigest(), count, self.root / f"{digest.hexdigest()}.raw"
            )
            try:
                os.link(partial, source.path)
            except FileExistsError:
                self.verify(source)
            partial.unlink()
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return source

    def _check_receipt(self, source: ArchivedSource) -> None:
        if source.path != self.root / f"{source.sha256}.raw":
            raise StorageError("invalid source archive receipt or path")

    def _verify_stream(self, source: ArchivedSource, stream: BinaryIO) -> None:
        before = _identity(os.fstat(stream.fileno()))
        # A separate reader bypasses bytes cached before a concurrent file mutation.
        with os.fdopen(os.dup(stream.fileno()), "rb") as verification:
            verification.seek(0)
            if _digest(verification) != (source.sha256, source.byte_count):
                raise StorageError("archived source checksum or size mismatch")
        if before != _identity(os.fstat(stream.fileno())) or before != _identity(
            source.path.stat()
        ):
            raise StorageError("archived source changed during verification")

    def verify(self, source: ArchivedSource) -> None:
        self._check_receipt(source)
        with _open_regular(source.path) as stream:
            self._verify_stream(source, stream)

    @contextmanager
    def open_verified(self, source: ArchivedSource) -> Iterator[BinaryIO]:
        self._check_receipt(source)
        with _open_regular(source.path) as stream:
            self._verify_stream(source, stream)
            stream.seek(0)
            try:
                yield stream
            finally:
                self._verify_stream(source, stream)
