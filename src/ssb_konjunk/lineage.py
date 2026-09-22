import hashlib
import json
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
import os



class LineageTracker:
    """Tracks read files and writes lineage metadata."""

    def __init__(self) -> None:
        self.run_id = self._generate_run_id()
        self.inputs: dict[str, list[dict]] = {lineage_type: []}

    @staticmethod
    def _generate_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"{timestamp}_{short_uuid}"

    @staticmethod
    def _calculate_sha256(filepath: str) -> str:
        sha256 = hashlib.sha256()

        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

        return sha256.hexdigest()


    @staticmethod
    def _user_info() -> dict:
        return {
            "user": os.environ.get("DAPLA_USER")
        }
    
    @staticmethod
    def _git_info() -> dict:
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

    def register_input(
        self,
        filepath: str,
        lineage_type: str,
    ) -> None:

        path = Path(filepath)

        entry = {
            "path": str(path),
            "sha256": self._calculate_sha256(str(path)),
        }

        existing_paths = {
            item["path"]
            for item in self.inputs[lineage_type]
        }

        if entry["path"] not in existing_paths:
            self.inputs[lineage_type].append(entry)

    def write_lineage(
        self,
        output_file: str,
        lineage_type: str,
    ) -> None:

        lineage = {
            "run_id": self.run_id,
            "created_at": datetime.now(UTC).isoformat(),
            "user": self._user_info(),
            "inputs": self.inputs[lineage_type],
            "output": {
                "path": output_file,
                "sha256": self._calculate_sha256(output_file),
            },
            "git": self._git_info(),
        }

        lineage_file = f"{output_file}.lineage.json"

        with open(lineage_file, "w", encoding="utf-8") as f:
            json.dump(lineage, f, indent=4)


def register_input(
    filepath: str,
    lineage_type: str,
) -> None:
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
    if _tracker is None:
        return

    _tracker.write_lineage(
        output_file,
        lineage_type,
    )

def start_lineage_run() -> None:
    global _tracker
    _tracker = LineageTracker()
    
def stop_lineage_run() -> None:
    global _tracker
    _tracker = None