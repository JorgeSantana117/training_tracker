from __future__ import annotations

from typing import Tuple, Dict, Set, Iterable
import re
import unicodedata
import pandas as pd


def _normalize_text(s: object) -> str:
    """Normaliza texto para matching robusto (mayúsculas, sin acentos, espacios)."""
    if s is None:
        return ""
    s = str(s).strip().upper()
    # quitar acentos
    s = "".join(
        ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn"
    )
    # mantener letras/números/espacio/coma
    s = re.sub(r"[^A-Z0-9 ,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _user_key_from_user_name(user_name: object) -> str:
    """Convierte 'NAME, LAST NAME' -> key normalizada 'NAME, LAST NAME'."""
    s = _normalize_text(user_name)
    if "," in s:
        given, surn = [p.strip() for p in s.split(",", 1)]
        return f"{given}, {surn}"
    return s


def _candidate_user_keys_from_full_name(full_name: object, max_given_tokens: int = 3) -> Iterable[str]:
    """Genera posibles llaves 'GIVEN, SURNAMES' a partir de 'SURNAMES GIVEN'.

    Ej:
      'SANTANA MENDOZA JORGE' -> ['JORGE, SANTANA MENDOZA', 'MENDOZA JORGE, SANTANA', ...]
    """
    s = _normalize_text(full_name)
    tokens = [t for t in s.replace(",", " ").split() if t]
    if len(tokens) < 2:
        return []
    max_l = min(max_given_tokens, len(tokens) - 1)
    for l in range(1, max_l + 1):
        surn = " ".join(tokens[:-l]).strip()
        given = " ".join(tokens[-l:]).strip()
        if surn and given:
            yield f"{given}, {surn}"


def _is_yes(v: object) -> bool:
    s = _normalize_text(v)
    return s in {"YES", "Y", "TRUE", "1", "SI", "SÍ"}


def _add_completion_segment(df: pd.DataFrame) -> pd.DataFrame:
    """Añade columna segment basada en completion_pct."""
    df = df.copy()

    def seg(x):
        try:
            x = float(x)
        except Exception:
            return None
        if pd.isna(x):
            return None
        if x >= 90:
            return ">=90%"
        if x >= 70:
            return "70–89%"
        return "<70%"

    df["segment"] = df["completion_pct"].apply(seg)
    return df


def compute_employee_kpis(
    hr_df: pd.DataFrame,
    roles_df: pd.DataFrame,
    status_df: pd.DataFrame,
    reference_year: int | None = None,  # compatibilidad: ya no se usa
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calcula KPIs a nivel empleado y detalle Mandatory/Optional.

    Entradas (canónicas):
    - hr_df: full_name, job_title, org_code, org_desc, head_of_department
    - roles_df: org_code, org_desc, job_title, curriculum_id, curriculum_title, required_type, is_mandatory
    - status_df: user_name, org_desc, curriculum_id, curriculum_title, curriculum_complete (Yes/No), days_remaining

    Salidas:
    - employee_kpis_df (1 fila por empleado)
    - mandatory_detail_df (1 fila por (empleado, curriculum Mandatory) en Roles)
    - optional_detail_df  (1 fila por (empleado, curriculum Optional) en Roles)

    Reglas clave:
    - is_assigned = True si existe registro en Trainings_Report para (empleado, curriculum_id)
    - days_remaining:
        * vacío (NaN) si curriculum_completed=True
        * numérico si curriculum_completed=False (positivo = a tiempo, negativo = overdue)
        * vacío (NaN) si is_assigned=False
    - curriculum_done (bool):
        * True si curriculum_completed=True
        * True si curriculum_completed=False y days_remaining >= 0
        * False si curriculum_completed=False y days_remaining < 0
        * False si is_assigned=False

    NOTA: se usa days_remaining >= 0 (incluye 0 como "a tiempo").
    """
    hr_df = hr_df.copy()
    roles_df = roles_df.copy()
    status_df = status_df.copy()

    # tipos base
    for c in ["full_name", "job_title", "org_code", "org_desc", "head_of_department"]:
        if c in hr_df.columns:
            hr_df[c] = hr_df[c].astype(str).str.strip()
    for c in ["org_code", "org_desc", "job_title", "curriculum_id", "curriculum_title"]:
        if c in roles_df.columns:
            roles_df[c] = roles_df[c].astype(str).str.strip()
    for c in ["user_name", "org_desc", "curriculum_id", "curriculum_title", "curriculum_complete"]:
        if c in status_df.columns:
            status_df[c] = status_df[c].astype(str).str.strip()

    # asegurar is_mandatory
    if "is_mandatory" not in roles_df.columns and "required" in roles_df.columns:
        req_norm = roles_df["required"].astype(str).str.strip().str.lower()
        roles_df["is_mandatory"] = req_norm.map({"mandatory": True, "obligatorio": True, "optional": False}).fillna(False)

    # --- 1) Construir llave interna de empleado (para evitar choques por homónimos) ---
    hr_df["__org_desc_norm"] = hr_df["org_desc"].map(_normalize_text)
    hr_df["__employee_key"] = (
        hr_df["full_name"].map(_normalize_text)
        + "|"
        + hr_df["org_code"].map(_normalize_text)
        + "|"
        + hr_df["org_desc"].map(_normalize_text)
        + "|"
        + hr_df["job_title"].map(_normalize_text)
    )

    # --- 2) Mapear status.user_name -> employee_key usando (user_key, org_desc) ---
    key_to_emp: Dict[tuple[str, str], Set[str]] = {}
    for _, r in hr_df.iterrows():
        emp_key = r["__employee_key"]
        org_desc_norm = r["__org_desc_norm"]
        for cand in _candidate_user_keys_from_full_name(r["full_name"]):
            key = (cand, org_desc_norm)
            key_to_emp.setdefault(key, set()).add(emp_key)

    def resolve_emp_key(user_name: object, org_desc: object) -> str | None:
        k = (_user_key_from_user_name(user_name), _normalize_text(org_desc))
        s = key_to_emp.get(k, set())
        if len(s) == 1:
            return next(iter(s))
        return None

    status_df["__employee_key"] = status_df.apply(
        lambda r: resolve_emp_key(r.get("user_name"), r.get("org_desc")), axis=1
    )

    # curriculum_completed + days_remaining
    status_df["curriculum_completed"] = status_df["curriculum_complete"].apply(_is_yes)
    if "days_remaining" in status_df.columns:
        status_df["days_remaining"] = pd.to_numeric(status_df["days_remaining"], errors="coerce")
    else:
        status_df["days_remaining"] = pd.NA

    # --- 3) Requisitos por empleado: merge HR x Roles (org_code + org_desc + job_title) ---
    required_all = hr_df.merge(
        roles_df,
        on=["org_code", "org_desc", "job_title"],
        how="left",
        suffixes=("", "_role"),
    )
    required_all = required_all[~required_all["curriculum_id"].isna()].copy()
    required_all["curriculum_id"] = required_all["curriculum_id"].astype(str)

    # --- 4) Agregar status por (employee_key, curriculum_id) ---
    status_subset = status_df[["__employee_key", "curriculum_id", "curriculum_completed", "days_remaining"]].copy()
    status_subset = status_subset[~status_subset["__employee_key"].isna()].copy()
    status_subset["curriculum_id"] = status_subset["curriculum_id"].astype(str)

    status_agg = (
        status_subset.groupby(["__employee_key", "curriculum_id"], as_index=False)
        .agg(
            curriculum_completed=("curriculum_completed", "max"),
            days_remaining=("days_remaining", "min"),
            assigned_flag=("curriculum_id", "size"),
        )
    )
    status_agg["assigned_flag"] = 1  # por si el size no se conserva como int en algunos pandas

    req_with_status = required_all.merge(
        status_agg,
        on=["__employee_key", "curriculum_id"],
        how="left",
    )

    # is_assigned: True si existe registro en Trainings_Report
    req_with_status["is_assigned"] = req_with_status["assigned_flag"].fillna(0).astype(int).astype(bool)
    req_with_status = req_with_status.drop(columns=["assigned_flag"], errors="ignore")

    # curriculum_completed: bool
    req_with_status["curriculum_completed"] = req_with_status["curriculum_completed"].fillna(False).astype(bool)

    # days_remaining: vacío cuando no asignado o cuando ya completó
    req_with_status.loc[~req_with_status["is_assigned"], "days_remaining"] = pd.NA
    req_with_status.loc[req_with_status["curriculum_completed"], "days_remaining"] = pd.NA

    # curriculum_done: bool (ver reglas en docstring)
    req_with_status["curriculum_done"] = False
    assigned = req_with_status["is_assigned"]
    completed = req_with_status["curriculum_completed"]
    req_with_status.loc[assigned & completed, "curriculum_done"] = True
    mask_pending = assigned & (~completed)
    req_with_status.loc[mask_pending, "curriculum_done"] = (
        req_with_status.loc[mask_pending, "days_remaining"].astype("float").ge(0).fillna(False)
    )
    req_with_status["curriculum_done"] = req_with_status["curriculum_done"].astype(bool)

    # --- 5) Detalles: se separa Mandatory vs Optional ---
    base_detail_cols = [
        "__employee_key",
        "full_name",
        "job_title",
        "org_code",
        "org_desc",
        "head_of_department",
        "curriculum_id",
        "curriculum_title",
        "is_assigned",
        "curriculum_completed",
        "days_remaining",
        "curriculum_done",
    ]

    mandatory_detail_df = req_with_status[req_with_status["is_mandatory"] == True][base_detail_cols].copy()  # noqa: E712
    optional_detail_df = req_with_status[req_with_status["is_mandatory"] == False][base_detail_cols].copy()   # noqa: E712

    # Orden para detalles (sin la llave interna)
    detail_order = [c for c in base_detail_cols if c != "__employee_key"]
    mandatory_detail_df = mandatory_detail_df[detail_order].copy()
    optional_detail_df = optional_detail_df[detail_order].copy()

    # --- 6) KPIs por empleado (solo Mandatory para conteos principales) ---
    base_emp = hr_df[[
        "__employee_key",
        "full_name",
        "job_title",
        "org_code",
        "org_desc",
        "head_of_department",
    ]].copy()

    # agregados mandatory por currículo (para evitar dobles conteos)
    if not mandatory_detail_df.empty:
        mand_tmp = req_with_status[req_with_status["is_mandatory"] == True].copy()  # noqa: E712
        mand_curr = (
            mand_tmp.groupby(["__employee_key", "curriculum_id"], as_index=False)
            .agg(
                curriculum_completed=("curriculum_completed", "max"),
                curriculum_done=("curriculum_done", "max"),
                is_assigned=("is_assigned", "max"),
            )
        )
        mand_agg = mand_curr.groupby("__employee_key").agg(
            mandatory_count=("curriculum_id", "nunique"),
            mandatory_completed_count=("curriculum_completed", "sum"),
            mandatory_done_count=("curriculum_done", "sum"),
        ).reset_index()
        unassigned = (
            mand_curr.groupby("__employee_key")["is_assigned"]
            .apply(lambda s: int((~s.astype(bool)).sum()))
            .reset_index(name="unassigned_mandatory_count")
        )
        mand_agg = mand_agg.merge(unassigned, on="__employee_key", how="left")
    else:
        mand_agg = pd.DataFrame({"__employee_key": base_emp["__employee_key"]})
        mand_agg["mandatory_count"] = 0
        mand_agg["mandatory_completed_count"] = 0
        mand_agg["mandatory_done_count"] = 0
        mand_agg["unassigned_mandatory_count"] = 0

    # agregados Optional (solo completados)
    if not optional_detail_df.empty:
        opt_tmp = req_with_status[req_with_status["is_mandatory"] == False].copy()  # noqa: E712
        opt_curr = (
            opt_tmp.groupby(["__employee_key", "curriculum_id"], as_index=False)
            .agg(curriculum_completed=("curriculum_completed", "max"))
        )
        opt_agg = opt_curr.groupby("__employee_key").agg(
            optional_completed_count=("curriculum_completed", "sum"),
        ).reset_index()
    else:
        opt_agg = pd.DataFrame({"__employee_key": base_emp["__employee_key"]})
        opt_agg["optional_completed_count"] = 0

    employee_kpis_df = base_emp.merge(mand_agg, on="__employee_key", how="left")
    employee_kpis_df = employee_kpis_df.merge(opt_agg, on="__employee_key", how="left")

    # defaults/tipos
    for col in [
        "mandatory_count",
        "mandatory_completed_count",
        "mandatory_done_count",
        "unassigned_mandatory_count",
        "optional_completed_count",
    ]:
        employee_kpis_df[col] = employee_kpis_df[col].fillna(0).astype(int)

    employee_kpis_df["mandatory_missing_count"] = (
        employee_kpis_df["mandatory_count"] - employee_kpis_df["mandatory_completed_count"]
    ).astype(int)

    employee_kpis_df["has_requirements"] = employee_kpis_df["mandatory_count"] > 0

    employee_kpis_df["completion_pct"] = employee_kpis_df.apply(
        lambda r: (100.0 * r["mandatory_completed_count"] / r["mandatory_count"]) if r["mandatory_count"] > 0 else None,
        axis=1,
    ).astype(float)

    employee_kpis_df["full_compliance_flag"] = (
        employee_kpis_df["has_requirements"] & (employee_kpis_df["mandatory_missing_count"] == 0)
    ).astype(bool)

    employee_kpis_df["full_done_flag"] = (
        employee_kpis_df["has_requirements"] & (employee_kpis_df["mandatory_done_count"] == employee_kpis_df["mandatory_count"])
    ).astype(bool)

    # Orden final
    employee_kpis_df = employee_kpis_df[[
        "full_name",
        "job_title",
        "org_code",
        "org_desc",
        "head_of_department",
        "mandatory_count",
        "mandatory_completed_count",
        "mandatory_done_count",
        "mandatory_missing_count",
        "unassigned_mandatory_count",
        "has_requirements",
        "completion_pct",
        "full_compliance_flag",
        "full_done_flag",
        "optional_completed_count",
    ]].copy()

    return employee_kpis_df, mandatory_detail_df, optional_detail_df


def compute_department_kpis(employee_kpis_df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agregados por unidad (org_code + org_desc)."""
    df = _add_completion_segment(employee_kpis_df)

    dept = (
        df.groupby(["org_code", "org_desc"])
        .agg(
            employees_count=("full_name", "size"),
            employees_with_requirements=("has_requirements", "sum"),
            employees_full_compliance=("full_compliance_flag", "sum"),
            avg_completion_pct=("completion_pct", "mean"),
        )
        .reset_index()
    )

    # conteo segmentos
    seg_counts = (
        df.groupby(["org_code", "org_desc", "segment"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    for col in [">=90%", "70–89%", "<70%"]:
        if col not in seg_counts.columns:
            seg_counts[col] = 0

    dept = dept.merge(seg_counts[["org_code", "org_desc", ">=90%", "70–89%", "<70%"]], on=["org_code", "org_desc"], how="left")

    dept["dept_full_compliance_rate"] = dept.apply(
        lambda r: (r["employees_full_compliance"] / r["employees_with_requirements"]) if r["employees_with_requirements"] > 0 else None,
        axis=1,
    )

    # Orden exacto solicitado
    return dept[
        [
            "org_code",
            "org_desc",
            "employees_count",
            "employees_with_requirements",
            "employees_full_compliance",
            "avg_completion_pct",
            "70–89%",
            "<70%",
            ">=90%",
            "dept_full_compliance_rate",
        ]
    ]


def compute_organization_kpis(employee_kpis_df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agregados por organización (org_code)."""
    df = _add_completion_segment(employee_kpis_df)

    org = (
        df.groupby(["org_code"])
        .agg(
            employees_count=("full_name", "size"),
            employees_with_requirements=("has_requirements", "sum"),
            employees_full_compliance=("full_compliance_flag", "sum"),
            avg_completion_pct=("completion_pct", "mean"),
        )
        .reset_index()
    )

    seg_counts = (
        df.groupby(["org_code", "segment"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    for col in [">=90%", "70–89%", "<70%"]:
        if col not in seg_counts.columns:
            seg_counts[col] = 0

    org = org.merge(seg_counts[["org_code", ">=90%", "70–89%", "<70%"]], on=["org_code"], how="left")

    org["org_full_compliance_rate"] = org.apply(
        lambda r: (r["employees_full_compliance"] / r["employees_with_requirements"]) if r["employees_with_requirements"] > 0 else None,
        axis=1,
    )

    return org[
        [
            "org_code",
            "employees_count",
            "employees_with_requirements",
            "employees_full_compliance",
            "avg_completion_pct",
            "70–89%",
            "<70%",
            ">=90%",
            "org_full_compliance_rate",
        ]
    ]


def compute_company_kpis(employee_kpis_df: pd.DataFrame, mandatory_detail_df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agregados a nivel empresa (basados en Mandatory)."""
    df = employee_kpis_df.copy()

    total_employees = int(len(df))
    employees_with_requirements = int(df["has_requirements"].sum())
    employees_full_compliance = int(df["full_compliance_flag"].sum())
    avg_completion_pct = float(df["completion_pct"].mean()) if employees_with_requirements > 0 else None

    company_full_compliance_rate = (
        employees_full_compliance / employees_with_requirements if employees_with_requirements > 0 else None
    )

    total_mandatory_curricula = int(len(mandatory_detail_df))
    total_completed_mandatory = int(mandatory_detail_df["curriculum_completed"].sum()) if total_mandatory_curricula > 0 else 0
    mandatory_curricula_completion_rate = (
        (total_completed_mandatory / total_mandatory_curricula) if total_mandatory_curricula > 0 else None
    )

    data = {
        "total_employees": total_employees,
        "employees_with_requirements": employees_with_requirements,
        "employees_full_compliance": employees_full_compliance,
        "avg_completion_pct": avg_completion_pct,
        "company_full_compliance_rate": company_full_compliance_rate,
        "total_mandatory_curricula": total_mandatory_curricula,
        "total_completed_mandatory": total_completed_mandatory,
        "mandatory_curricula_completion_rate": mandatory_curricula_completion_rate,
    }

    return pd.DataFrame([data])