"""PreToolUse hook: bloquea Edit/Write/NotebookEdit de Claude sobre los Excel de
data/raw/ del proyecto y cualquier master*.csv (raíz o data/, incluidos backups).

Falla cerrado: entrada ilegible o sin ruta válida => bloquear (exit 2).
Límite: si Python no arranca, Claude Code trata el fallo como no bloqueante
(onFailure requiere Claude Code >= 2.1.295; instalado: 2.1.289).
Cobertura parcial declarada: no intercepta comandos Bash (python, cp, mv, rm,
redirecciones). Eso lo cubre la regla 2 de CLAUDE.md.
"""
import json
import os
import sys

EXCEL_EXT = (".xlsx", ".xls", ".xlsm", ".xlsb")


def block(msg: str) -> int:
    print(msg, file=sys.stderr)
    return 2


def inside(path: str, folder: str) -> bool:
    if os.path.splitdrive(path)[0] != os.path.splitdrive(folder)[0]:
        return False
    return os.path.commonpath([path, folder]) == folder


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    try:
        data = json.load(sys.stdin)
    except Exception as e:
        return block(f"Hook protect_data: entrada ilegible ({type(e).__name__}); se bloquea por seguridad.")
    if not isinstance(data, dict) or not isinstance(data.get("tool_input"), dict):
        return block("Hook protect_data: evento sin tool_input; se bloquea por seguridad.")

    ti = data["tool_input"]
    raw = ti.get("file_path") or ti.get("notebook_path")
    if not isinstance(raw, str) or not raw.strip():
        return block("Hook protect_data: ruta ausente o inválida; se bloquea por seguridad.")

    base = data["cwd"] if isinstance(data.get("cwd"), str) and data["cwd"] else os.getcwd()
    project = os.environ.get("CLAUDE_PROJECT_DIR") or base
    path = os.path.normcase(os.path.normpath(os.path.join(base, raw)))
    raw_dir = os.path.normcase(os.path.normpath(os.path.join(project, "data", "raw")))
    name = os.path.basename(path)

    is_raw_excel = inside(path, raw_dir) and name.endswith(EXCEL_EXT)
    is_master = name.startswith("master") and name.endswith(".csv")

    if is_raw_excel or is_master:
        return block(
            f"Bloqueado: {raw} solo cambia corriendo el pipeline (CLAUDE.md, regla 2). "
            "Usa /pipeline-dry y pide OK a Eli."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
