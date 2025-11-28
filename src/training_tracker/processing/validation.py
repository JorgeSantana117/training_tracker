from __future__ import annotations

from typing import List, Dict
import re
import unicodedata
import pandas as pd


def _normalize_text(s: object) -> str:
    if s is None:
        return ""
    s = str(s).strip().upper()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^A-Z0-9 ,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def validate_data(
    hr_df: pd.DataFrame,
    roles_df: pd.DataFrame,
    status_df: pd.DataFrame,
    allowed_completion_status: List[str] | None = None,  # compatibilidad
) -> List[Dict]:
    """Validaciones básicas sobre los datos de entrada (adaptadas a nuevo layout).

    Devuelve lista de issues con:
      - level: ERROR | WARNING
      - code
      - message
      - details (dict)
    """
    issues: List[Dict] = []

    # --- HR ---
    hr_needed = {"full_name", "job_title", "org_code", "org_desc", "head_of_department"}
    missing = sorted(list(hr_needed - set(hr_df.columns)))
    if missing:
        issues.append(
            {
                "level": "ERROR",
                "code": "HR_MISSING_COLUMNS",
                "message": f"HR: faltan columnas esperadas: {missing}",
                "details": {"missing": missing},
            }
        )
        return issues

    # llave interna
    hr_keys = (
        hr_df["full_name"].map(_normalize_text)
        + "|"
        + hr_df["org_code"].map(_normalize_text)
        + "|"
        + hr_df["org_desc"].map(_normalize_text)
        + "|"
        + hr_df["job_title"].map(_normalize_text)
    )
    dup = hr_keys[hr_keys.duplicated()].unique().tolist()
    if dup:
        issues.append(
            {
                "level": "ERROR",
                "code": "HR_DUPLICATE_EMPLOYEE_KEY",
                "message": (
                    "HR: se detectaron empleados duplicados (mismo Full Name + Org + Org Desc + Job Title). "
                    "Con la eliminación de Local ID esto vuelve ambigua la relación."
                ),
                "details": {"duplicate_keys_sample": dup[:10], "count": len(dup)},
            }
        )

    # --- Roles ---
    roles_needed = {"org_code", "org_desc", "job_title", "curriculum_id", "curriculum_title"}
    roles_missing = sorted(list(roles_needed - set(roles_df.columns)))
    if roles_missing:
        issues.append(
            {
                "level": "ERROR",
                "code": "ROLES_MISSING_COLUMNS",
                "message": f"Roles: faltan columnas esperadas: {roles_missing}",
                "details": {"missing": roles_missing},
            }
        )

    # --- Status ---
    status_needed = {"user_name", "org_desc", "curriculum_id", "curriculum_title", "curriculum_complete", "days_remaining"}
    status_missing = sorted(list(status_needed - set(status_df.columns)))
    if status_missing:
        issues.append(
            {
                "level": "ERROR",
                "code": "STATUS_MISSING_COLUMNS",
                "message": f"Status: faltan columnas esperadas: {status_missing}",
                "details": {"missing": status_missing},
            }
        )
        return issues

    # --- Empleados sin requisitos (por join org_code+org_desc+job_title) ---
    # Si roles no trae columnas, evitar excepción
    if not roles_missing:
        hr_pairs = hr_df[["org_code", "org_desc", "job_title"]].drop_duplicates()
        roles_pairs = roles_df[["org_code", "org_desc", "job_title"]].drop_duplicates()
        merged = hr_pairs.merge(roles_pairs, on=["org_code", "org_desc", "job_title"], how="left", indicator=True)
        no_req = merged[merged["_merge"] == "left_only"]
        if not no_req.empty:
            issues.append(
                {
                    "level": "WARNING",
                    "code": "HR_EMPLOYEE_WITHOUT_ROLE_REQUIREMENTS",
                    "message": (
                        f"{len(no_req)} combinaciones (org_code, org_desc, job_title) en HR no tienen requisitos en Roles. "
                        "Estos empleados saldrán con required_count=0."
                    ),
                    "details": {"sample": no_req.head(10).to_dict(orient="records")},
                }
            )

    # --- Status sin match probable contra HR (por nombre+org_desc) ---
    # Heurística: contar cuántos user_name/org_desc aparecen en HR al menos por org_desc
    hr_org_desc_set = set(hr_df["org_desc"].map(_normalize_text).unique().tolist())
    status_org_desc_norm = status_df["org_desc"].map(_normalize_text)
    bad_org_desc = status_df[~status_org_desc_norm.isin(hr_org_desc_set)]
    if not bad_org_desc.empty:
        issues.append(
            {
                "level": "WARNING",
                "code": "STATUS_ORG_DESC_NOT_IN_HR",
                "message": (
                    f"{len(bad_org_desc)} filas de Status tienen Organization Description que no existe en HR. "
                    "Es probable que no se puedan asignar a un empleado."
                ),
                "details": {"sample": bad_org_desc.head(10)[["user_name", "org_desc"]].to_dict(orient="records")},
            }
        )

    return issues
