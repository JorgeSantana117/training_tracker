from pathlib import Path
import pandas as pd
from .utils import normalize_columns

def load_roles(input_dir: Path) -> pd.DataFrame:
    """Fusiona todos los archivos de Roles en un solo DataFrame.

    Columnas de salida:
    - org_code        (Organization, ej. ABC/XYZ/RST)
    - org_desc        (Organization Description: unidad/sub-organización, ej. BG/ABC1)
    - job_title
    - curriculum_id
    - curriculum_title
    - required_type   (texto original, ej. 'Mandatory' / 'Optional')
    - is_mandatory    (bool: True si es entrenamiento obligatorio)
    """
    org_root = input_dir / "organizations"
    all_rows = []

    if not org_root.exists():
        raise ValueError(f"No existe la carpeta de organizations: {org_root}")

    for org_dir in org_root.iterdir():
        if not org_dir.is_dir():
            continue
        role_dir = org_dir / "Roles"
        if not role_dir.exists():
            continue
        for xlsx in role_dir.glob("*.xlsx"):
            df = pd.read_excel(xlsx)
            df = normalize_columns(df)
            # Limpieza suave de columnas de organización
            if "organization_description" in df.columns:
                df["organization_description"] = df["organization_description"].astype(str).str.strip()
            if "organization" in df.columns:
                df["organization"] = df["organization"].astype(str).str.strip()

            # org_code: código de organización (ABC/XYZ/RST)
            if "organization" in df.columns:
                df["org_code"] = df["organization"].astype(str)
            elif "organization_name" in df.columns:
                df["org_code"] = df["organization_name"].astype(str)
            else:
                df["org_code"] = org_dir.name

            # org_desc: unidad / sub-organización
            if "organization_description" in df.columns:
                df["org_desc"] = df["organization_description"].astype(str)
            else:
                df["org_desc"] = ""

            if "curriculum_id" not in df.columns:
                raise ValueError(f"Roles: falta 'curriculum_id' en {xlsx}")

            # Columna Required: Mandatory / Optional / NA (o vacío)
            if "required" in df.columns:
                req = df["required"].astype("string").str.strip()
            else:
                # compatibilidad hacia atrás si el archivo no trae la columna
                req = pd.Series(["Mandatory"] * len(df), dtype="string")

            req_key = req.str.lower()

            # Ignorar cursos no relevantes: Required=NA, N/A, vacío o celdas en blanco (NaN)
            drop_mask = req_key.isna() | req_key.isin(["na", "n/a", "nan", ""])
            keep_mask = req_key.isin(["mandatory", "optional", "obligatorio"])

            unknown = set(req_key[~drop_mask & ~keep_mask].dropna().unique())
            if unknown:
                raise ValueError(f"Roles: valores desconocidos en 'Required' {sorted(unknown)} en {xlsx}")

            # filtra SOLO Mandatory/Optional (ignora NA/blank)
            df = df.loc[keep_mask].copy()
            req_key = req_key.loc[keep_mask]

            if df.empty:
                # este archivo/unidad no aporta requerimientos analizables
                continue

            required_type = req_key.map(
                {"mandatory": "Mandatory", "optional": "Optional", "obligatorio": "Mandatory"}
            ).astype("string")

            is_mandatory = required_type.eq("Mandatory")
            tmp = pd.DataFrame()
            tmp["org_code"] = df["org_code"]
            tmp["org_desc"] = df["org_desc"]
            tmp["job_title"] = df["job_title"]
            tmp["curriculum_id"] = df["curriculum_id"].astype(str)
            tmp["curriculum_title"] = df["curriculum_title"].astype(str)
            tmp["required_type"] = required_type
            tmp["is_mandatory"] = is_mandatory

            all_rows.append(tmp)

    if not all_rows:
        raise ValueError("No se encontraron archivos de Roles")

    roles_df = pd.concat(all_rows, ignore_index=True).drop_duplicates()
    return roles_df
