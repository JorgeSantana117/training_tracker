import sys
import pandas as pd
import click

from .config import get_settings
from .io.hr_loader import load_hr, load_curriculum_list
from .io.roles_loader import load_roles
from .io.status_loader import load_status
from .processing.validation import validate_data
from .processing.completion_calc import (
    compute_employee_kpis,
    compute_department_kpis,
    compute_company_kpis,
    compute_organization_kpis,
)
from .reporting.exporters import export_all


@click.group()
def cli():
    """TrainingTracker CLI."""
    pass


def _load_all():
    settings = get_settings()
    input_dir = settings.input_dir

    hr_df = load_hr(input_dir)
    roles_df = load_roles(input_dir)
    status_df = load_status(input_dir)

    # --- Curriculum List (whitelist) ---
    curriculum_df = load_curriculum_list(input_dir)
    curriculum_df["curriculum_id"] = curriculum_df["curriculum_id"].astype(str).str.strip()
    allowed = set(curriculum_df["curriculum_id"].tolist())
    title_map = dict(zip(curriculum_df["curriculum_id"], curriculum_df["curriculum_title"]))

    # Filtra Roles/Status a la lista permitida
    roles_df["curriculum_id"] = roles_df["curriculum_id"].astype(str).str.strip()
    status_df["curriculum_id"] = status_df["curriculum_id"].astype(str).str.strip()

    roles_df = roles_df[roles_df["curriculum_id"].isin(allowed)].copy()
    status_df = status_df[status_df["curriculum_id"].isin(allowed)].copy()

    # Homologar títulos con la lista oficial
    if "curriculum_title" in roles_df.columns:
        roles_df["curriculum_title"] = roles_df["curriculum_id"].map(title_map).fillna(roles_df["curriculum_title"])
    else:
        roles_df["curriculum_title"] = roles_df["curriculum_id"].map(title_map)

    if "curriculum_title" in status_df.columns:
        status_df["curriculum_title"] = status_df["curriculum_id"].map(title_map).fillna(status_df["curriculum_title"])
    else:
        status_df["curriculum_title"] = status_df["curriculum_id"].map(title_map)

    return settings, hr_df, roles_df, status_df


@cli.command()
def validate():
    """Valida los inputs y muestra el resultado por consola."""
    settings, hr_df, roles_df, status_df = _load_all()

    issues = validate_data(
        hr_df=hr_df,
        roles_df=roles_df,
        status_df=status_df,
        allowed_completion_status=settings.allowed_completion_status,
    )

    if not issues:
        click.echo("✅ No se encontraron problemas en las validaciones.")
        sys.exit(0)

    click.echo("⚠️ Se encontraron los siguientes problemas:")
    for issue in issues:
        click.echo(f"- [{issue['level']}] {issue['code']}: {issue['message']}")

    if any(i["level"] == "ERROR" for i in issues):
        sys.exit(1)
    else:
        sys.exit(0)


@cli.command()
def build_outputs():
    """Valida, calcula KPIs y genera outputs en la carpeta de output/."""
    settings, hr_df, roles_df, status_df = _load_all()

    issues = validate_data(
        hr_df=hr_df,
        roles_df=roles_df,
        status_df=status_df,
        allowed_completion_status=settings.allowed_completion_status,
    )

    has_errors = any(i["level"] == "ERROR" for i in issues)
    output_dir = settings.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    issues_df = pd.DataFrame(issues) if issues else None
    if has_errors:
        click.echo("❌ Hay errores de validación. Revisa el comando `validate`.")
        if issues_df is not None:
            issues_df.to_csv(output_dir / "validation_issues.csv", index=False)
        sys.exit(1)

    click.echo("✅ Validaciones OK. Calculando KPIs...")

    employee_kpis_df, required_detail_df, extra_detail_df = compute_employee_kpis(
        hr_df=hr_df,
        roles_df=roles_df,
        status_df=status_df,
        reference_year=settings.reference_year,
    )
    dept_kpis_df = compute_department_kpis(employee_kpis_df)
    org_kpis_df = compute_organization_kpis(employee_kpis_df)
    company_kpis_df = compute_company_kpis(employee_kpis_df, required_detail_df)

    export_all(
        output_dir=output_dir,
        employee_kpis_df=employee_kpis_df,
        required_detail_df=required_detail_df,
        extra_detail_df=extra_detail_df,
        dept_kpis_df=dept_kpis_df,
        company_kpis_df=company_kpis_df,
        org_kpis_df=org_kpis_df,
        validation_issues=issues_df,
    )

    click.echo(f"📂 Outputs generados en: {output_dir}")


if __name__ == "__main__":
    cli()
