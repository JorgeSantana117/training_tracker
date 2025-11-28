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
    """Calcula KPIs a nivel empleado y detalle requerido/extra.

    Entradas (canónicas):
    - hr_df: full_name, job_title, org_code, org_desc, head_of_department
    - roles_df: org_code, org_desc, job_title, curriculum_id, curriculum_title, is_mandatory
    - status_df: user_name, org_desc, curriculum_id, curriculum_title, curriculum_complete (Yes/No), days_remaining

    Salidas:
    - employee_kpis_df (1 fila por empleado)
    - required_detail_df (1 fila por (empleado, curriculum en roles))
    - extra_detail_df (1 fila por (empleado, curriculum completado no presente en roles))
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

    if "is_mandatory" not in roles_df.columns and "required" in roles_df.columns:
        # fallback: si por alguna razón roles_df viene sin is_mandatory pero con "required"
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

    status_df["curriculum_completed"] = status_df["curriculum_complete"].apply(_is_yes)

    # Days Remaining: numérico (puede venir vacío).
    if "days_remaining" in status_df.columns:
        status_df["days_remaining"] = pd.to_numeric(status_df["days_remaining"], errors="coerce")
    else:
        # por si el loader aún no lo incluyó (defensivo)
        status_df["days_remaining"] = pd.NA

    # Regla: si el currículo ya está completado, Days Remaining debe quedar vacío.
    status_df.loc[status_df["curriculum_completed"] == True, "days_remaining"] = pd.NA

    # curriculum_done (booleano): True si completado, o si no completado pero NO está overdue (days_remaining >= 0).
    status_df["curriculum_done"] = (
        status_df["curriculum_completed"]
        | ((~status_df["curriculum_completed"]) & status_df["days_remaining"].notna() & (status_df["days_remaining"] >= 0))
    ).astype(bool)
    status_completed = status_df[status_df["curriculum_completed"] == True].copy()  # noqa: E712

    # --- 3) Requisitos por empleado: merge HR x Roles (org_code + org_desc + job_title) ---
    required = hr_df.merge(
        roles_df,
        on=["org_code", "org_desc", "job_title"],
        how="left",
        suffixes=("", "_role"),
    )

    required = required[~required["curriculum_id"].isna()].copy()
    required["curriculum_id"] = required["curriculum_id"].astype(str)

    # --- 4) Unir requisitos con status por (employee_key, curriculum_id) ---
    status_subset = status_df[["__employee_key", "curriculum_id", "curriculum_completed", "days_remaining"]].copy()
    status_subset = status_subset[~status_subset["__employee_key"].isna()].copy()
    status_subset["curriculum_id"] = status_subset["curriculum_id"].astype(str)
    # Deduplicar por si hay múltiples registros del mismo currículo por empleado
    # - curriculum_completed: True si cualquiera está completado
    # - days_remaining: mínimo (más cercano / más overdue), ignorando NaN
    status_subset = (
        status_subset.groupby(["__employee_key", "curriculum_id"], as_index=False)
        .agg(
            curriculum_completed=("curriculum_completed", "max"),
            days_remaining=("days_remaining", "min"),
        )
    )
    req_with_status = required.merge(
        status_subset,
        on=["__employee_key", "curriculum_id"],
        how="left",
    )
    # is_assigned: True si existe registro en Trainings_Report para (empleado, curriculum)
    req_with_status["is_assigned"] = req_with_status["curriculum_completed"].notna()
    req_with_status["curriculum_completed"] = req_with_status["curriculum_completed"].fillna(False).astype(bool)

    # curriculum_done (según Days Remaining + Curriculum Completed)
    # - Si está completado: True
    # - Si NO está completado y days_remaining >= 0: True (aún en tiempo)
    # - Si NO está completado y days_remaining < 0: False (overdue)
    # - Si NO está asignado: mantener days_remaining vacío y curriculum_done=False
    req_with_status["curriculum_done"] = (
        req_with_status["curriculum_completed"]
        | (
            req_with_status["is_assigned"]
            & (~req_with_status["curriculum_completed"])
            & req_with_status["days_remaining"].notna()
            & (req_with_status["days_remaining"] >= 0)
        )
    ).astype(bool)

    # --- 5) Agregados por empleado ---
    agg_req = (
        req_with_status.groupby("__employee_key")
        .agg(
            required_count=("curriculum_id", "nunique"),
            completed_required_count=("curriculum_completed", "sum"),
            done_required_count=("curriculum_done", "sum"),
        )
        .reset_index()
    )
    # mandatorios sin asignar (no hay registro en Trainings_Report)
    unassigned = (
        req_with_status[(req_with_status["is_mandatory"] == True) & (~req_with_status["is_assigned"])]
        .groupby("__employee_key")
        .size()
        .rename("unassigned_mandatory_count")
        .reset_index()
    )
    agg_req = agg_req.merge(unassigned, on="__employee_key", how="left")
    agg_req["unassigned_mandatory_count"] = agg_req["unassigned_mandatory_count"].fillna(0).astype(int)

    agg_req["missing_required_count"] = agg_req["required_count"] - agg_req["completed_required_count"]
    agg_req["has_requirements"] = agg_req["required_count"] > 0
    agg_req["completion_pct"] = agg_req.apply(
        lambda r: (100.0 * r["completed_required_count"] / r["required_count"]) if r["required_count"] > 0 else None,
        axis=1,
    )
    agg_req["full_compliance_flag"] = agg_req["has_requirements"] & (agg_req["missing_required_count"] == 0)
    agg_req["full_done_flag"] = agg_req["has_requirements"] & (agg_req["done_required_count"] == agg_req["required_count"])

    # --- 6) Employee KPIs (incluye empleados sin requisitos) ---
    base_emp = hr_df[
        [
            "__employee_key",
            "full_name",
            "job_title",
            "org_code",
            "org_desc",
            "head_of_department",
        ]
    ].copy()

    employee_kpis_df = base_emp.merge(agg_req, on="__employee_key", how="left")

    for col in ["required_count", "completed_required_count", "done_required_count", "missing_required_count", "unassigned_mandatory_count"]:
        employee_kpis_df[col] = employee_kpis_df[col].fillna(0).astype(int)
    employee_kpis_df["has_requirements"] = employee_kpis_df["required_count"] > 0
    employee_kpis_df["completion_pct"] = employee_kpis_df["completion_pct"].astype(float)
    employee_kpis_df["full_compliance_flag"] = employee_kpis_df["full_compliance_flag"].fillna(False).astype(bool)
    employee_kpis_df["full_done_flag"] = employee_kpis_df["full_done_flag"].fillna(False).astype(bool)

    # --- 7) Detalle requerido ---
    required_detail_df = req_with_status[
        [
            "__employee_key",
            "full_name",
            "job_title",
            "org_code",
            "org_desc",
            "head_of_department",
            "curriculum_id",
            "curriculum_title",
            "is_mandatory",
            "is_assigned",
            "curriculum_completed",
            "days_remaining",
            "curriculum_done",
        ]
    ].copy()

    # --- 8) Extra (completados que no aparecen en Roles para el empleado) ---
    required_keys = set(zip(required["__employee_key"].astype(str), required["curriculum_id"].astype(str)))

    status_completed = status_completed[~status_completed["__employee_key"].isna()].copy()
    status_completed["curriculum_id"] = status_completed["curriculum_id"].astype(str)

    def _is_extra(row) -> bool:
        return (str(row["__employee_key"]), str(row["curriculum_id"])) not in required_keys

    status_completed["__is_extra"] = status_completed.apply(_is_extra, axis=1)
    extra_detail_df = status_completed[status_completed["__is_extra"]].copy()

    # Añadir columnas de HR
    extra_detail_df = extra_detail_df.merge(
        hr_df[
            [
                "__employee_key",
                "full_name",
                "job_title",
                "org_code",
                "org_desc",
                "head_of_department",
            ]
        ],
        on="__employee_key",
        how="left",
        suffixes=("", "_hr"),
    )
    extra_detail_df["is_mandatory"] = False

    extra_detail_df = extra_detail_df[
        [
            "full_name",
            "job_title",
            "org_code",
            "org_desc",
            "head_of_department",
            "curriculum_id",
            "curriculum_title",
            "is_mandatory",
            "curriculum_completed",
            "days_remaining",
            "curriculum_done",
        ]
    ].copy()

    # --- 9) extra_completed_count ---
    if extra_detail_df.empty:
        employee_kpis_df["extra_completed_count"] = 0
    else:
        ex = (
            extra_detail_df.groupby(["full_name", "org_code", "org_desc", "job_title"])  # estable aunque haya homónimos
            ["curriculum_id"]
            .nunique()
            .reset_index(name="extra_completed_count")
        )
        employee_kpis_df = employee_kpis_df.merge(
            ex,
            on=["full_name", "org_code", "org_desc", "job_title"],
            how="left",
        )
        employee_kpis_df["extra_completed_count"] = employee_kpis_df["extra_completed_count"].fillna(0).astype(int)

    # Reindex/orden exacto solicitado para employee_kpis
    employee_kpis_df = employee_kpis_df[
        [
            "full_name",
            "job_title",
            "org_code",
            "org_desc",
            "head_of_department",
            "required_count",
            "completed_required_count",
            "done_required_count",
            "missing_required_count",
            "unassigned_mandatory_count",
            "has_requirements",
            "completion_pct",
            "full_compliance_flag",
            "full_done_flag",
            "extra_completed_count",
        ]
    ].copy()

    # Orden para detalles
    required_detail_df = required_detail_df[
        [
            "full_name",
            "job_title",
            "org_code",
            "org_desc",
            "head_of_department",
            "curriculum_id",
            "curriculum_title",
            "is_mandatory",
            "is_assigned",
            "curriculum_completed",
            "days_remaining",
            "curriculum_done",
        ]
    ].copy()

    # En extra_detail_df ya viene con el orden correcto

    return employee_kpis_df, required_detail_df, extra_detail_df


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


def compute_company_kpis(employee_kpis_df: pd.DataFrame, required_detail_df: pd.DataFrame) -> pd.DataFrame:
    """KPIs agregados a nivel empresa."""
    df = employee_kpis_df.copy()

    total_employees = int(len(df))
    employees_with_requirements = int(df["has_requirements"].sum())
    employees_full_compliance = int(df["full_compliance_flag"].sum())
    avg_completion_pct = float(df["completion_pct"].mean()) if employees_with_requirements > 0 else None

    company_full_compliance_rate = (
        employees_full_compliance / employees_with_requirements if employees_with_requirements > 0 else None
    )

    total_required_curricula = int(len(required_detail_df))
    total_completed_required = int(required_detail_df["curriculum_completed"].sum()) if total_required_curricula > 0 else 0
    curricula_completion_rate = (
        (total_completed_required / total_required_curricula) if total_required_curricula > 0 else None
    )

    data = {
        "total_employees": total_employees,
        "employees_with_requirements": employees_with_requirements,
        "employees_full_compliance": employees_full_compliance,
        "avg_completion_pct": avg_completion_pct,
        "company_full_compliance_rate": company_full_compliance_rate,
        "total_required_curricula": total_required_curricula,
        "total_completed_required": total_completed_required,
        "curricula_completion_rate": curricula_completion_rate,
    }

    return pd.DataFrame([data])
