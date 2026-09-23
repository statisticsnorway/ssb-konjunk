import hashlib
import json
import os
import subprocess
import uuid

import fsspec
import pendulum

_tracker = None

VALID_LINEAGE_TYPES = {
    "production",
}


class LineageTracker:
    """Tracks read files and writes lineage metadata."""

    def __init__(self) -> None:
        self.run_id = self._generate_run_id()

        self.inputs: dict[str, list[dict]] = {
            lineage_type: [] for lineage_type in VALID_LINEAGE_TYPES
        }

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
            str: a uniuque 22 long string.
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

    def register_input(
        self,
        filepath: str,
        lineage_type: str,
    ) -> None:
        """Register an input file in the lineage log.

        Stores the file path and SHA-256 hash.
        If the file has already been registered for that lineage
        type, it is not added again.

        Args:
            filepath: Path to the input file.
            lineage_type: Lineage category to register the file under.
                Reserved for future lineage-specific functionality.
        """
        entry = {
            "path": filepath,
            "sha256": self._calculate_sha256(filepath),
        }

        existing_paths = {item["path"] for item in self.inputs[lineage_type]}

        if entry["path"] not in existing_paths:
            self.inputs[lineage_type].append(entry)

    def write_lineage(
        self,
        output_file: str,
        lineage_type: str,
    ) -> None:
        """Write a lineage log for an output file.

        Creates a lineage log containing run information, input files,
        output file details, Git metadata, user information, and any
        additional metadata. The lineage log is written as a JSON file
        alongside the output file.

        Args:
            output_file: Path to the output file.
            lineage_type: Lineage category used to select the registered
                input files. Reserved for future lineage-specific
                functionality
        """
        lineage = {
            "run_id": self.run_id,
            "created_at": pendulum.now().format("YYYY-MM-DD HH:mm:ss"),
            "user": self._user_info(),
            "inputs": self.inputs[lineage_type],
            "output": {
                "path": output_file,
                "sha256": self._calculate_sha256(output_file),
            },
            "git": self._git_info(),
            "metadata": self.metadata,
        }

        lineage_file = f"{output_file}.lineage.json"

        with open(lineage_file, "w", encoding="utf-8") as f:
            json.dump(lineage, f, indent=4)


def register_input(
    filepath: str,
    lineage_type: str,
) -> None:
    """Register an input file in the active lineage run.

    If no lineage run is active, the function does nothing.

    Args:
        filepath: Path to the input file.
        lineage_type: Lineage category to register the file under.
            Reserved for future lineage-specific functionality.
    """
    if _tracker is None:
        return

    _tracker.register_input(
        filepath,
        lineage_type,
    )


def write_lineage(
    output_file: str,
    lineage_type: str,
) -> None:
    """Write a lineage log for an output file.

    If no lineage run is active, the function does nothing.

    Args:
        output_file: Path to the output file.
        lineage_type: Lineage category used to select the registered
            input files. Reserved for future lineage-specific
            functionality.
    """
    if _tracker is None:
        return

    _tracker.write_lineage(
        output_file,
        lineage_type,
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


def start_lineage_run() -> None:
    """Start a new lineage run.

    Creates a new lineage tracker and initializes a unique run ID.
    Any previously active lineage run is replaced.
    """
    global _tracker
    _tracker = LineageTracker()


def stop_lineage_run() -> None:
    """Stop the active lineage run.

    Removes the current lineage tracker. Subsequent lineage
    operations become no-ops until a new lineage run is started.
    """
    global _tracker
    _tracker = None
