from pathlib import Path
import pandas as pd


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def export_all(
    output_dir: Path,
    employee_kpis_df: pd.DataFrame,
    mandatory_detail_df: pd.DataFrame,
    optional_detail_df: pd.DataFrame,
    dept_kpis_df: pd.DataFrame,
    company_kpis_df: pd.DataFrame,
    org_kpis_df: pd.DataFrame,
    validation_issues: pd.DataFrame | None = None,
) -> None:
    """Exporta todos los DataFrames a un solo archivo Excel con múltiples hojas."""
    ensure_output_dir(output_dir)

    xlsx_path = output_dir / "training_tracker_outputs.xlsx"

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        employee_kpis_df.to_excel(writer, sheet_name="employee_kpis", index=False)
        mandatory_detail_df.to_excel(writer, sheet_name="employee_mandatory_detail", index=False)
        optional_detail_df.to_excel(writer, sheet_name="employee_optional_detail", index=False)
        dept_kpis_df.to_excel(writer, sheet_name="department_kpis", index=False)
        org_kpis_df.to_excel(writer, sheet_name="organization_kpis", index=False)
        company_kpis_df.to_excel(writer, sheet_name="company_kpis", index=False)

        if validation_issues is not None and not validation_issues.empty:
            validation_issues.to_excel(writer, sheet_name="validation_issues", index=False)
