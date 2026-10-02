import hashlib
import json
import subprocess

import pytest

import ssb_konjunk.lineage as lineage


@pytest.fixture(autouse=True)
def mock_git(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "call",
        lambda *args, **kwargs: 0,
    )

    monkeypatch.setattr(
        subprocess,
        "check_output",
        lambda *args, **kwargs: "dummy\n",
    )


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


def test_add_metadata(tmp_path):
    lineage_file = tmp_path / "test-lineage.json"
    tracker = lineage.LineageTracker(lineage_file)

    tracker.add_metadata("table_id", 123)

    assert tracker.metadata == {"table_id": 123}


def test_register_input(tmp_path):
    file = tmp_path / "input.txt"
    file.write_text("hello world")

    lineage_file = tmp_path / "test-lineage.json"
    tracker = lineage.LineageTracker(lineage_file)

    tracker.register_input(
        filepath=str(file),
    )

    assert len(tracker.inputs) == 1
    assert tracker.inputs[0]["path"] == str(file)


def test_register_input_duplicate(tmp_path):
    file = tmp_path / "input.txt"
    file.write_text("hello")

    lineage_file = tmp_path / "test-lineage.json"
    tracker = lineage.LineageTracker(lineage_file)

    tracker.register_input(str(file))
    tracker.register_input(str(file))

    assert len(tracker.inputs) == 1


def test_register_output(tmp_path):
    file = tmp_path / "output.txt"
    file.write_text("hello world")

    lineage_file = tmp_path / "test-lineage.json"
    tracker = lineage.LineageTracker(lineage_file)

    tracker.register_output(
        filepath=str(file),
    )

    assert len(tracker.outputs) == 1
    assert tracker.outputs[0]["path"] == str(file)


def test_write_lineage_content(tmp_path, monkeypatch):

    input_file = tmp_path / "input.txt"
    input_file.write_text("input")

    output_file = tmp_path / "output.txt"
    output_file.write_text("output")
    lineage_file = tmp_path / "lineage.json"
    tracker = lineage.LineageTracker(lineage_file)

    tracker.register_input(
        str(input_file),
    )

    tracker.add_metadata(
        "dataset",
        "123",
    )

    tracker.register_output(
        str(output_file),
    )

    with open(
        lineage_file.with_suffix(".json"),
        encoding="utf-8",
    ) as f:
        lineage_dict = json.load(f)
    run = next(iter(lineage_dict.values()))
    assert run["metadata"]["dataset"] == "123"
    assert run["git"]["commit"] == "dummy"
    assert len(run["inputs"]) == 1


def test_start_lineage_run(tmp_path):
    lineage_file = tmp_path / "test-lineage.json"
    lineage.start_lineage_run(lineage_file)

    assert lineage._tracker is not None


def test_stop_lineage_run(tmp_path):
    lineage_file = tmp_path / "test-lineage.json"
    lineage.LineageTracker(lineage_file)

    lineage.stop_lineage_run()

    assert lineage._tracker is None


def test_register_input_no_tracker(tmp_path):
    lineage._tracker = None

    file = tmp_path / "input.txt"
    file.write_text("hello")

    lineage.register_input(
        str(file),
    )
