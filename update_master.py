"""
update_master.py — Escritura idempotente a master.csv
Share Dashboard · Investor Reporting Pipeline

Responsabilidad ÚNICA: recibir el DataFrame procesado y escribirlo
a master.csv con control de duplicados por clave primaria.

Idempotencia garantizada: correr 2 veces con el mismo input
produce exactamente el mismo master.csv.

NO procesa. NO lee Excel. NO construye visualizaciones.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Clave primaria — debe coincidir con config.yaml
PRIMARY_KEY = ["pais", "categoria", "sub_categoria", "laboratorio", "producto"]


# ---------------------------------------------------------------------------
# CARGA DE CONFIG
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"config.yaml no encontrado: {path.absolute()}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# LÓGICA DE MERGE IDEMPOTENTE
# ---------------------------------------------------------------------------

def merge_with_master(
    df_new: pd.DataFrame,
    master_path: Path,
    pk_cols: list[str],
) -> tuple[pd.DataFrame, dict]:
    """
    Combina el DataFrame nuevo con el master existente.

    Lógica:
        - Si master no existe: el nuevo DataFrame es el master.
        - Si master existe: elimina filas cuya clave primaria existe en df_new,
          luego concatena df_new. Resultado: df_new siempre gana (upsert).

    Args:
        df_new:      DataFrame procesado y validado de process.py.
        master_path: ruta al master.csv (puede no existir).
        pk_cols:     lista de columnas que forman la clave primaria.
    Returns:
        Tuple(DataFrame_merged, stats_dict).
    """
    stats = {
        "registros_nuevos":       0,
        "registros_reemplazados": 0,
        "registros_anteriores":   0,
        "total_final":            0,
    }

    if not master_path.exists():
        logger.info("master.csv no existe — creando desde cero")
        stats["registros_nuevos"] = len(df_new)
        stats["total_final"] = len(df_new)
        return df_new.copy(), stats

    logger.info(f"Leyendo master existente: {master_path}")
    df_master = pd.read_csv(master_path, low_memory=False)
    stats["registros_anteriores"] = len(df_master)

    # Identificar cuántas claves del master serán reemplazadas
    pk_new = df_new[pk_cols].apply(tuple, axis=1)
    pk_master = df_master[pk_cols].apply(tuple, axis=1)
    overlap_mask = pk_master.isin(set(pk_new))

    stats["registros_reemplazados"] = overlap_mask.sum()
    stats["registros_nuevos"] = len(df_new) - stats["registros_reemplazados"]

    # Conservar del master solo los que NO están en el nuevo
    df_master_keep = df_master[~overlap_mask].copy()

    # Alinear columnas (el nuevo puede tener columnas adicionales)
    all_cols = list(dict.fromkeys(list(df_master_keep.columns) + list(df_new.columns)))
    df_master_keep = df_master_keep.reindex(columns=all_cols)
    df_new_aligned = df_new.reindex(columns=all_cols)

    df_merged = pd.concat([df_master_keep, df_new_aligned], ignore_index=True)
    stats["total_final"] = len(df_merged)

    return df_merged, stats


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def update_master(
    df_processed: pd.DataFrame,
    config: dict,
    backup: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Escribe df_processed a master.csv con idempotencia garantizada.

    Args:
        df_processed: DataFrame validado de process.py.
        config:       configuración completa de config.yaml.
        backup:       si True, crea backup del master anterior antes de sobrescribir.
        dry_run:      si True, simula sin escribir a disco (útil para debugging).
    Returns:
        Dict con estadísticas de la operación.
    """
    master_path = Path(config["paths"]["master"])
    master_path.parent.mkdir(parents=True, exist_ok=True)

    pk_cols = [c for c in PRIMARY_KEY if c in df_processed.columns]

    logger.info("=== INICIO UPDATE MASTER ===")
    logger.info(f"  Destino: {master_path}")
    logger.info(f"  Registros entrantes: {len(df_processed):,}")
    logger.info(f"  Dry run: {dry_run}")

    # Backup del master anterior
    if backup and master_path.exists() and not dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = master_path.with_name(f"master_backup_{timestamp}.csv")
        import shutil
        shutil.copy2(master_path, backup_path)
        logger.info(f"  Backup creado: {backup_path}")

    # Merge idempotente
    df_final, stats = merge_with_master(df_processed, master_path, pk_cols)

    # Escribir a disco
    if not dry_run:
        df_final.to_csv(master_path, index=False, encoding="utf-8-sig")
        logger.info(f"  master.csv escrito: {len(df_final):,} registros")
    else:
        logger.info("  [DRY RUN] — no se escribió a disco")

    # Log de estadísticas
    logger.info(
        f"=== UPDATE COMPLETADO ===\n"
        f"  Registros anteriores:   {stats['registros_anteriores']:>6,}\n"
        f"  Registros reemplazados: {stats['registros_reemplazados']:>6,}\n"
        f"  Registros nuevos:       {stats['registros_nuevos']:>6,}\n"
        f"  TOTAL FINAL:            {stats['total_final']:>6,}"
    )

    return stats


# ---------------------------------------------------------------------------
# SMOKE TEST — ejecutar con: python scripts/update_master.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from ingest import ingest_shares_file, load_config
    from process import process

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smoke test de update_master.py")
    parser.add_argument("--file",    required=True, help="Excel de shares")
    parser.add_argument("--fx",      required=True, help="Excel tipo de cambio")
    parser.add_argument("--config",  default="config.yaml")
    parser.add_argument("--sheets",  nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Simular sin escribir a disco")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SMOKE TEST — update_master.py | {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    try:
        config = load_config(args.config)
        raw = ingest_shares_file(args.file, config, sheets_override=args.sheets)
        df_proc, report = process(raw, config, fx_path=args.fx)
        stats = update_master(df_proc, config, dry_run=args.dry_run)

        print(f"\n{'✅ DRY RUN OK' if args.dry_run else '✅ MASTER ACTUALIZADO'}")
        print(f"{'─'*40}")
        for k, v in stats.items():
            print(f"  {k:<30} {v:>8,}")

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(str(e))
        sys.exit(1)
