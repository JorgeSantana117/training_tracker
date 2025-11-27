from pathlib import Path
import pandas as pd
from .utils import normalize_columns


def load_hr(input_dir: Path) -> pd.DataFrame:
    """Lee hr/Associates_List.xlsx y regresa un DF canónico.

    NUEVO (Nov-2025): ya no existe 'Local ID'. La relación con el resto de tablas
    se hace por 'Full Name'.

    Columnas de salida:
    - full_name
    - job_title
    - org_code   (Organization)
    - org_desc   (Organization Description: nombre/descr. de la unidad)
    - head_of_department
    """
    path = input_dir / "hr" / "Associates_List.xlsx"
    df = pd.read_excel(path)

    df = df.rename(columns={
    "Org Unit Abbr": "Organization Description"})

    df = normalize_columns(df)

    # Limpieza suave
    if "organization_description" in df.columns:
        df["organization_description"] = df["organization_description"].astype(str).str.strip()
    if "organization" in df.columns:
        df["organization"] = df["organization"].astype(str).str.strip()
    if "full_name" in df.columns:
        df["full_name"] = df["full_name"].astype(str).str.strip()

    rename_map = {
        "organization": "org_code",
        "organization_description": "org_desc",
    }
    df = df.rename(columns=rename_map)

    needed = [
        "full_name",
        "job_title",
        "org_code",
        "org_desc",
        "head_of_department",
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"HR: faltan columnas esperadas: {missing}")

    df["org_code"] = df["org_code"].astype(str)
    df["org_desc"] = df["org_desc"].astype(str)

    return df[needed]
