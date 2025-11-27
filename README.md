# TrainingTracker

Herramienta en Python para calcular y exportar KPIs de cumplimiento de entrenamientos por:

- **Empresa**
- **Organización / Unidad**
- **Empleado**

A partir de tres tipos de entradas:

- **HR** – Lista de asociados
- **Roles** – Requisitos de entrenamientos por rol
- **Status** – Estado de currícula por empleado

El objetivo es generar salidas listas para consumo en herramientas de BI (por ejemplo, Power BI) y que la solución pueda migrarse fácil a pipelines de CI/CD en el futuro.

---

## 1. Estructura del proyecto

```text
training_tracker_repo/
├─ src/
│  └─ training_tracker/
│     ├─ __init__.py
│     ├─ config.py
│     ├─ cli.py
│     ├─ io/
│     │  ├─ hr_loader.py
│     │  ├─ roles_loader.py
│     │  └─ status_loader.py
│     ├─ processing/
│     │  ├─ completion_calc.py
│     │  └─ validation.py
│     └─ reporting/
│        └─ exporters.py
├─ tests/
├─ requirements.txt
└─ run_training_tracker.bat
"# training_tracker" 
# training_tracker
# training_tracker
# training_tracker
