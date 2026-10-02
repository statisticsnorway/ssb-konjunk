import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path

import fsspec
import pendulum

_tracker = None


class LineageTracker:
    """Tracks read files and writes lineage metadata."""

    def __init__(
        self,
        lineage_file: str,
    ) -> None:
        """Initialize a new lineage tracker.

        Creates the 'abse' of the dict that is used in the lineage log.

        Args:
            lineage_file: the filelocation of the created lineage log file.
        """
        self.lineage_file = lineage_file
        try:
            with fsspec.open(
                lineage_file,
                "r",
                encoding="utf-8",
            ) as f:
                self.lineage_history = json.load(f)
        except FileNotFoundError:
            self.lineage_history = {}

        self.run_id = self._generate_run_id()
        self.created_at = pendulum.now().format("YYYY-MM-DD HH:mm:ss")
        self.user_info = self._user_info()
        self.inputs: list[dict] = []
        self.outputs: list[dict] = []
        self.git_info = self._git_info()
        self.metadata: dict[str, object] = {}

    @staticmethod
    def _generate_run_id() -> str:
        """Function to generate an unique for each lineage file.

        Returns:
            str: a uniuque 22 long string.
        """
        timestamp = pendulum.now().format("YYYYMMDDHHmmss")
        short_uuid = str(uuid.uuid4())[:8]
        return f"{timestamp}{short_uuid}"

    @staticmethod
    def _calculate_sha256(filepath: str) -> str:
        """Function to generate an unique hash for each lineage file.

        Returns:
            str: SHA-256 hash of the file
        """
        sha256 = hashlib.sha256()
        with fsspec.open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    @staticmethod
    def _user_info() -> dict:
        """Function to generate the user that is running the program.

        Returns:
            str: a string with the users 3 letter mail.
        """
        return {"user": os.environ.get("DAPLA_USER")}

    @staticmethod
    def _git_info() -> dict:
        """Function to show what git repo, and branch that is beeing used.

        Returns:
            dict[str, str]: Metadata containing the remote
            repository URL, current branch, and commit hash.

        Raises:
            RuntimeError: If the Git repository contains uncommitted changes.
        """
        dirty = bool(
            subprocess.call(
                ["git", "diff", "--quiet"],
            )
        )

        if dirty:
            raise RuntimeError(
                "Git repository contains uncommitted changes. Commit or stash changes before creating lineage."
            )

        return {
            "repo": subprocess.check_output(
                ["git", "config", "--get", "remote.origin.url"],
                text=True,
            ).strip(),
            "branch": subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                text=True,
            ).strip(),
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                text=True,
            ).strip(),
        }

    def _flush(self) -> None:
        """Function to update the lineagelogging file everytime a file or metadata is added.

        overwrites the file on disk with the most updated version of the dict any time its updated, so it possible to read 'unfinished' runs.

        """
        lineage = {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "user": self.user_info,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "git": self.git_info,
            "metadata": self.metadata,
        }

        self.lineage_history[self.run_id] = lineage

        with fsspec.open(
            self.lineage_file,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(
                self.lineage_history,
                f,
                indent=4,
            )

    def add_metadata(
        self,
        key: str,
        value: object,
    ) -> None:
        """Add custom metadata to the lineage log.

        The metadata is included in the lineage log when it is written.

        Args:
            key: Metadata field name.
            value: Metadata value to store.
        """
        self.metadata[key] = value
        self._flush()

    def _register_file(
        self,
        filepath: str,
        storage: list[dict],
    ) -> None:
        """Registers a file to the lineage log.

        Args:
            filepath: the path to the file that is beeing logged.
            storage: if its an output or an input file.

        """
        entry = {
            "path": filepath,
            "sha256": self._calculate_sha256(filepath),
        }
        existing_paths = {item["path"] for item in storage}

        if entry["path"] not in existing_paths:
            storage.append(entry)
            self._flush()

    def register_input(
        self,
        filepath: str,
    ) -> None:
        """Function to register an input to the lineage logger.

        Args:
            filepath: the path to the file that is beeing logged.
        """
        self._register_file(
            filepath,
            self.inputs,
        )

    def register_output(
        self,
        filepath: str,
    ) -> None:
        """Function to register an output to the lineage logger.

        Args:
            filepath: the path to the file that is beeing logged.
        """
        self._register_file(
            filepath,
            self.outputs,
        )


def register_input(
    filepath: str,
) -> None:
    """Register an input file in the active lineage run.

    If no lineage run is active, the function does nothing.

    Args:
        filepath: Path to the input file.
    """
    if _tracker is None:
        return

    _tracker.register_input(
        filepath,
    )


def register_output(
    filepath: str,
) -> None:
    """Register an output file in the active lineage run.

    If no lineage run is active, the function does nothing.

    Args:
        filepath: Path to the input file.
    """
    if _tracker is None:
        return

    _tracker.register_output(
        filepath,
    )


def add_lineage_metadata(
    key: str,
    value: object,
) -> None:
    """Add custom metadata to the active lineage run.

    If no lineage run is active, the function does nothing.

    Args:
        key: Metadata field name.
        value: Metadata value to store.
    """
    if _tracker is None:
        return

    _tracker.add_metadata(key, value)


def start_lineage_run(
    lineage_file: str,
) -> None:
    """Start a new lineage run.

    Creates a new lineage tracker and initializes a unique run ID.
    Any previously active lineage run is replaced.
    """
    global _tracker
    Path(lineage_file).parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    _tracker = LineageTracker(lineage_file)


def stop_lineage_run() -> None:
    """Stop the active lineage run.

    Removes the current lineage tracker.
    """
    global _tracker
    _tracker = None
