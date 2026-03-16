import glob
import json
from dataclasses import dataclass
from dataclasses import field
from typing import Literal

# 2.16.1

AGG_TYPES = Literal["SUMMED", "AVERAGE"]


@dataclass
class DatasetConfig:
    glob_pattern: str
    index_col: str
    index_pattern: str
    groupby_col: str | None = field(default=None)
    agg_type: AGG_TYPES | None = field(default=None)
    agg_type_by_col: dict[str, AGG_TYPES] | None = field(default=None)

    def get_entries(self):
        return glob.glob(self.glob_pattern)


def load_datasets(config_path: str) -> dict[str, DatasetConfig]:
    data: dict = json.load(open(config_path))
    datasets: dict[str, DatasetConfig] = {}
    for key, value in data.items():
        datasets[key] = DatasetConfig(**value)

    return datasets
