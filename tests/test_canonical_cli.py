"""Canonical and explicit limited-fidelity CLI integration."""

import subprocess
import sys
from pathlib import Path

import pytest

from quantlab.validation import main

ARGS = [
    "--format",
    "canonical",
    "--capture-id",
    "00000000-0000-4000-8000-000000000001",
    "--provider",
    "fixture-provider",
    "--provider-symbol",
    "fixture-symbol",
    "--pip-size",
    "2",
]


def test_canonical_fixture_summary(fixture_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([str(fixture_dir / "canonical_ticks.csv"), *ARGS]) == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert "market_tick.v1" in output.out
    assert "Validated records: 4" in output.out
    assert "BOOM_1000" in output.out
    assert "Minimum price: 999.90" in output.out


def test_canonical_command_installed_outside_workspace(fixture_dir: Path, tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "quantlab.validation",
            str(fixture_dir / "canonical_ticks.csv"),
            *ARGS,
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Validated records: 4" in result.stdout


def test_legacy_command_identifies_limited_fidelity(
    fixture_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main([str(fixture_dir / "valid_ticks.csv")]) == 0
    output = capsys.readouterr()
    assert "legacy" in output.out.lower()
    assert "limited fidelity" in output.out.lower()
    assert "market_tick.v1" not in output.out


def test_canonical_missing_configuration(
    fixture_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main([str(fixture_dir / "canonical_ticks.csv"), "--format", "canonical"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "INVALID_SOURCE_METADATA" in output.err


def test_canonical_rejection_has_code_no_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(
        "instrument_id,provider_symbol,event_time_utc,price,pip_size\n"
        "BOOM_1000,fixture-symbol,2026-01-01T00:00:00Z,0,2\n"
    )
    assert main([str(path), *ARGS]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "INVALID_PRICE" in output.err
    assert "Row 2" in output.err
