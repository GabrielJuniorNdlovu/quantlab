import hashlib
from dataclasses import replace

import pytest

from quantlab.storage.contracts import StorageError
from quantlab.storage.source_archive import SourceArchive


def test_source_bytes_preserved_deduplicated_and_read_only(tmp_path):
    original = tmp_path / "observations.csv"
    payload = b'head\r\n"quoted\nrow",\xe2\x82\xac\r\n'
    original.write_bytes(payload)
    archive = SourceArchive(tmp_path / "archive")
    source = archive.preserve(original)
    assert source.sha256 == hashlib.sha256(payload).hexdigest()
    assert source.byte_count == len(payload)
    assert source.path.read_bytes() == payload and original.read_bytes() == payload
    assert not source.path.stat().st_mode & 0o222
    assert archive.preserve(original) == source
    with archive.open_verified(source) as stream:
        assert stream.read() == payload
    assert list((tmp_path / "archive").glob("*.raw")) == [source.path]
    assert not list((tmp_path / "archive").glob(".partial-*"))


def test_existing_archive_is_verified_and_never_overwritten(tmp_path):
    original = tmp_path / "source"
    original.write_bytes(b"original")
    archive = SourceArchive(tmp_path / "archive")
    source = archive.preserve(original)
    source.path.chmod(0o644)
    source.path.write_bytes(b"corrupt")
    with pytest.raises(StorageError, match="checksum|size"):
        archive.preserve(original)
    assert source.path.read_bytes() == b"corrupt"
    with pytest.raises(StorageError, match="checksum|size"):
        archive.verify(source)


def test_receipt_path_hash_and_size_checked(tmp_path):
    source_file = tmp_path / "source"
    source_file.write_bytes(b"data")
    archive = SourceArchive(tmp_path / "archive")
    source = archive.preserve(source_file)
    with pytest.raises(StorageError):
        archive.verify(replace(source, sha256="0" * 64))
    with pytest.raises(StorageError):
        archive.verify(replace(source, byte_count=100))
    with pytest.raises(StorageError):
        archive.verify(replace(source, path=source_file))


def test_mutation_during_verified_read_is_detected(tmp_path):
    original = tmp_path / "source"
    original.write_bytes(b"original")
    archive = SourceArchive(tmp_path / "archive")
    source = archive.preserve(original)
    with pytest.raises(StorageError, match="checksum|size|changed"):
        with archive.open_verified(source) as stream:
            assert stream.read(1) == b"o"
            source.path.chmod(0o644)
            source.path.write_bytes(b"changed!")


def test_incomplete_copy_never_publishes_and_partial_is_identifiable(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"data")
    archive = SourceArchive(tmp_path / "archive")

    def failure(*args, **kw):
        raise OSError("publication interrupted")

    monkeypatch.setattr("quantlab.storage.source_archive.os.link", failure)
    with pytest.raises(OSError, match="publication interrupted"):
        archive.preserve(source)
    assert list((tmp_path / "archive").glob("*.raw")) == []
    assert len(list((tmp_path / "archive").glob(".partial-*"))) == 1


def test_source_change_during_copy_is_rejected(tmp_path, monkeypatch):
    original = tmp_path / "source"
    original.write_bytes(b"a" * 2000000)
    archive = SourceArchive(tmp_path / "archive")
    import quantlab.storage.source_archive as module

    real_fsync = module.os.fsync
    altered = False

    def change(fd):
        nonlocal altered
        if not altered:
            altered = True
            original.write_bytes(b"changed")
        real_fsync(fd)

    monkeypatch.setattr(module.os, "fsync", change)
    with pytest.raises(StorageError, match="changed"):
        archive.preserve(original)
    assert list((tmp_path / "archive").glob("*.raw")) == []


def test_missing_and_nonregular_source_fail_explicitly(tmp_path):
    archive = SourceArchive(tmp_path / "archive")
    with pytest.raises(FileNotFoundError):
        archive.preserve(tmp_path / "missing")
    with pytest.raises(StorageError, match="regular"):
        archive.preserve(tmp_path)
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    assert archive.preserve(empty).byte_count == 0
