import glob
import json
from dataclasses import dataclass
from dataclasses import field
from typing import Literal

# 2.16.1

AGG_TYPES = Literal["SUMMED", "AVERAGE"]


@dataclass
class DatasetConfig:
    """Konfigurasjon for innlasting av ett datasett.

    Klassen beskriver hvordan et datasett skal lokaliseres, hvilken kolonne
    som representerer tidsindeks, samt hvordan data eventuelt skal grupperes
    og aggregeres.
    """

    glob_pattern: str
    index_col: str
    index_pattern: str
    groupby_col: str | None = field(default=None)
    agg_type: Literal["SUMMED", "AVERAGE"] = "SUMMED"
    agg_type_by_col: dict[str, AGG_TYPES] | None = field(default=None)

    def get_entries(self) -> list:
        """Returnerer en liste med filer som matcher glob-mønsteret."""
        return glob.glob(self.glob_pattern)


def load_datasets(config_path: str) -> dict[str, DatasetConfig]:
    """Leser dataset-konfigurasjoner fra en JSON-fil.

    JSON-filen forventes å inneholde et objekt der hver nøkkel representerer
    et datasett, og verdien inneholder parameterne til `DatasetConfig`.

    Args:
        config_path (str): Filsti til JSON-konfigurasjonen.

    Returns:
        dict[str, DatasetConfig]: Ordbok der nøkkelen er datasettnavnet og verdien
        er en `DatasetConfig`-instans.
    """
    data: dict = json.load(open(config_path))
    datasets: dict[str, DatasetConfig] = {}
    for key, value in data.items():
        datasets[key] = DatasetConfig(**value)

    return datasets
