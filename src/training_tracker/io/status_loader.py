from pathlib import Path
import pandas as pd
from .utils import normalize_columns


def load_status(input_dir: Path) -> pd.DataFrame:
    """Fusiona todos los archivos de Status (Trainings Report) en un solo DataFrame.

    NUEVO (Nov-2025):
    - Se eliminan: Local ID, First Name, Last Name, Completion Date
    - Se agrega: User Name (formato: 'NAME, LAST NAME')
    - 'Completion Status' se renombra a 'Curriculum Complete' con valores: Yes/No

    Columnas de salida:
    - user_name
    - org_desc
    - curriculum_id
    - curriculum_title
    - curriculum_complete  (texto: Yes/No)
    - days_remaining      (numérico; vacío si Curriculum Completed=Yes)
    """
    org_root = input_dir / "organizations"
    all_rows = []

    if not org_root.exists():
        raise ValueError(f"No existe la carpeta de organizations: {org_root}")

    for org_dir in org_root.iterdir():
        if not org_dir.is_dir():
            continue

        status_dir = org_dir / "Status"
        if not status_dir.exists():
            continue

        for xlsx in status_dir.glob("*.xlsx"):
            df = pd.read_excel(xlsx)
            df = df.rename(columns={
                "Organization": "Organization Description"})
            df = normalize_columns(df)

            # Limpieza suave de cols de organización
            if "organization_description" in df.columns:
                df["organization_description"] = df["organization_description"].astype(str).str.strip()

            # curriculum_id puede venir como cirriculum_id
            if "curriculum_id" not in df.columns and "cirriculum_id" in df.columns:
                df["curriculum_id"] = df["cirriculum_id"]

            # compatibilidad: algunos archivos viejos usaban completion_status
            if "curriculum_complete" not in df.columns and "completion_status" in df.columns:
                df["curriculum_complete"] = df["completion_status"]

            # compatibilidad: algunos archivos traen 'Curriculum Completed'
            if "curriculum_complete" not in df.columns and "curriculum_completed" in df.columns:
                df["curriculum_complete"] = df["curriculum_completed"]

            # Days Remaining puede venir como 'Days Remaining' o 'Day Remaining'
            if "days_remaining" not in df.columns and "day_remaining" in df.columns:
                df["days_remaining"] = df["day_remaining"]

            needed_raw = [
                "user_name",
                "organization_description",
                "curriculum_id",
                "curriculum_title",
                "curriculum_complete",
                "days_remaining",
            ]
            missing = [c for c in needed_raw if c not in df.columns]
            if missing:
                raise ValueError(f"Status: faltan columnas {missing} en {xlsx}")

            tmp = pd.DataFrame()
            tmp["user_name"] = df["user_name"].astype(str).str.strip()
            tmp["org_desc"] = df["organization_description"].astype(str)
            tmp["curriculum_id"] = df["curriculum_id"].astype(str)
            tmp["curriculum_title"] = df["curriculum_title"].astype(str)
            tmp["curriculum_complete"] = df["curriculum_complete"].astype(str).str.strip()

            # days_remaining: numérico (puede venir vacío)
            tmp["days_remaining"] = pd.to_numeric(df["days_remaining"], errors="coerce")

            all_rows.append(tmp)

    if not all_rows:
        raise ValueError("No se encontraron archivos de Status")

    status_df = pd.concat(all_rows, ignore_index=True)
    return status_df
