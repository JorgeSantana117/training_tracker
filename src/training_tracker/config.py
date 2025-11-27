from dataclasses import dataclass
from pathlib import Path
import os
from typing import List
from datetime import datetime

@dataclass
class Settings:
    input_dir: Path
    output_dir: Path
    allowed_completion_status: List[str]
    reference_year: int

def get_settings() -> Settings:
    """Lee configuración desde variables de entorno o valores por defecto.

    - TRAINING_TRACKER_INPUT_DIR: ruta a la carpeta input/
    - TRAINING_TRACKER_OUTPUT_DIR: ruta a la carpeta output/
    - TRAINING_TRACKER_ALLOWED_STATUS: lista separada por ';'
      por ejemplo: "COMPLETED;IN PROGRESS;NOT STARTED;OVERDUE"
    - TRAINING_TRACKER_YEAR: año de referencia para considerar cursos completados.
      Si no se especifica, se usa el año actual.
    """

    cwd = Path.cwd()
    default_input = cwd / "training_tracker" / "input"
    default_output = cwd / "training_tracker" / "output"

    input_dir = Path(os.getenv("TRAINING_TRACKER_INPUT_DIR", default_input))
    output_dir = Path(os.getenv("TRAINING_TRACKER_OUTPUT_DIR", default_output))

    allowed_raw = os.getenv(
        "TRAINING_TRACKER_ALLOWED_STATUS",
        "COMPLETED;IN PROGRESS;NOT STARTED;OVERDUE",
    )
    allowed_completion_status = [
        s.strip() for s in allowed_raw.split(";") if s.strip()
    ]

    year_raw = os.getenv("TRAINING_TRACKER_YEAR")
    if year_raw:
        try:
            reference_year = int(year_raw)
        except ValueError:
            reference_year = datetime.today().year
    else:
        reference_year = datetime.today().year

    return Settings(
        input_dir=input_dir,
        output_dir=output_dir,
        allowed_completion_status=allowed_completion_status,
        reference_year=reference_year,
    )
