import hashlib
import json
import subprocess

import pytest

import ssb_konjunk.lineage as lineage


def test_generate_run_id():
    run_id = lineage.LineageTracker._generate_run_id()

    assert isinstance(run_id, str)
    assert len(run_id) == 22


def test_calculate_sha256(tmp_path):
    file = tmp_path / "input.txt"
    file.write_text("hello world")

    expected = hashlib.sha256(b"hello world").hexdigest()

    result = lineage.LineageTracker._calculate_sha256(str(file))

    assert result == expected


def test_user_info(monkeypatch):
    monkeypatch.setenv("DAPLA_USER", "hvr@ssb.no")

    result = lineage.LineageTracker._user_info()

    assert result == {"user": "hvr@ssb.no"}


def test_git_info(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "call",
        lambda *args, **kwargs: 0,
    )

    values = iter(
        [
            "repo-url\n",
            "main\n",
            "abc123\n",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "check_output",
        lambda *args, **kwargs: next(values),
    )

    result = lineage.LineageTracker._git_info()

    assert result == {
        "repo": "repo-url",
        "branch": "main",
        "commit": "abc123",
    }


def test_git_info_dirty_repo(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "call",
        lambda *args, **kwargs: 1,
    )

    with pytest.raises(
        RuntimeError,
        match="Git repository contains uncommitted changes",
    ):
        lineage.LineageTracker._git_info()


def test_add_metadata():
    tracker = lineage.LineageTracker()

    tracker.add_metadata("table_id", 123)

    assert tracker.metadata == {"table_id": 123}


def test_register_input(tmp_path):
    file = tmp_path / "input.txt"
    file.write_text("hello world")

    tracker = lineage.LineageTracker()

    tracker.register_input(
        filepath=str(file),
        lineage_type="production",
    )

    assert len(tracker.inputs["production"]) == 1
    assert tracker.inputs["production"][0]["path"] == str(file)


def test_register_input_duplicate(tmp_path):
    file = tmp_path / "input.txt"
    file.write_text("hello")

    tracker = lineage.LineageTracker()

    tracker.register_input(str(file), "production")
    tracker.register_input(str(file), "production")

    assert len(tracker.inputs["production"]) == 1


def test_write_lineage(tmp_path, monkeypatch):
    input_file = tmp_path / "input.txt"
    input_file.write_text("input")

    output_file = tmp_path / "output.txt"
    output_file.write_text("output")

    tracker = lineage.LineageTracker()

    monkeypatch.setattr(
        tracker,
        "_git_info",
        lambda: {
            "repo": "repo",
            "branch": "main",
            "commit": "abc",
        },
    )

    tracker.register_input(
        str(input_file),
        "production",
    )

    tracker.write_lineage(
        str(output_file),
        "production",
    )

    lineage_file = tmp_path / "output.txt.lineage.json"

    assert lineage_file.exists()


def test_write_lineage_content(tmp_path, monkeypatch):
    input_file = tmp_path / "input.txt"
    input_file.write_text("input")

    output_file = tmp_path / "output.txt"
    output_file.write_text("output")

    tracker = lineage.LineageTracker()

    monkeypatch.setattr(
        tracker,
        "_git_info",
        lambda: {
            "repo": "repo",
            "branch": "main",
            "commit": "abc",
        },
    )

    tracker.register_input(
        str(input_file),
        "production",
    )

    tracker.add_metadata(
        "dataset",
        "123",
    )

    tracker.write_lineage(
        str(output_file),
        "production",
    )

    with open(
        output_file.with_suffix(".txt.lineage.json"),
        encoding="utf-8",
    ) as f:
        lineage_dict = json.load(f)

    assert lineage_dict["metadata"]["dataset"] == "123"
    assert lineage_dict["git"]["commit"] == "abc"
    assert len(lineage_dict["inputs"]) == 1


def test_start_lineage_run():
    lineage.start_lineage_run()

    assert lineage._tracker is not None


def test_stop_lineage_run():
    lineage.start_lineage_run()

    lineage.stop_lineage_run()

    assert lineage._tracker is None

def test_register_input_no_tracker(tmp_path):
    lineage._tracker = None

    file = tmp_path / "input.txt"
    file.write_text("hello")

    lineage.register_input(
    str(file),
    "production",
    )

def test_write_lineage_no_tracker():
    lineage._tracker = None

    lineage.write_lineage(
        "output.parquet",
        "production",
    )

def test_write_lineage_no_tracker():
    lineage._tracker = None

    lineage.write_lineage(
        "output.parquet",
        "production",
    )