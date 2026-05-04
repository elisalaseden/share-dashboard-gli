"""
run_pipeline_pcbev.py — Orquestador del pipeline PC/Bebidas Brasil
Share Dashboard · Investor Reporting Pipeline

Punto de entrada mensual para PC/Bebidas.
Encadena: ingest_pcbev → process_pcbev → write master_pcbev.csv

Uso:
    python scripts/run_pipeline_pcbev.py \\
        --files data/raw/02_2026_Brasil_PC_Beverages_Base_Shares.xlsx \\
                data/raw/03_2026_Brasil_PC_Beverages_Base_Shares.xlsx \\
        --fx    data/raw/2026_03_Tipos_de_Cambio.xlsx

    # Dry run (sin escribir a disco):
    python scripts/run_pipeline_pcbev.py --files ... --fx ... --dry-run
"""

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')


# ---------------------------------------------------------------------------
# CLAVE PRIMARIA PC/BEV — sin sub_categoria
# ---------------------------------------------------------------------------

PK_PCBEV = ["pais", "categoria", "laboratorio", "producto"]


# ---------------------------------------------------------------------------
# SETUP LOGGING
# ---------------------------------------------------------------------------

def setup_logging(config: dict) -> None:
    log_dir = Path(config["paths"].get("log_dir", "logs/"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_pcbev_{datetime.now():%Y%m%d_%H%M%S}.log"

    logging.basicConfig(
        level=logging.INFO,
        format=config["logging"]["format"],
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.info(f"Log activo: {log_file}")


# ---------------------------------------------------------------------------
# ESCRITURA IDEMPOTENTE A MASTER_PCBEV.CSV
# ---------------------------------------------------------------------------

def write_master_pcbev(
    df_processed: pd.DataFrame,
    master_path: Path,
    backup: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Escribe df_processed a master_pcbev.csv con idempotencia garantizada.

    Lógica upsert:
        - Si master no existe: crear desde cero.
        - Si existe: eliminar filas cuya PK está en df_processed, concatenar.
          df_processed siempre gana (revisión más reciente).

    Args:
        df_processed: DataFrame validado de process_pcbev.
        master_path:  ruta a master_pcbev.csv.
        backup:       crear backup antes de sobrescribir.
        dry_run:      simular sin escribir a disco.
    Returns:
        Dict con estadísticas de la operación.
    """
    logger = logging.getLogger("write_master_pcbev")
    master_path.parent.mkdir(parents=True, exist_ok=True)

    pk_cols = [c for c in PK_PCBEV if c in df_processed.columns]
    stats = {
        "registros_anteriores":   0,
        "registros_reemplazados": 0,
        "registros_nuevos":       0,
        "total_final":            0,
    }

    logger.info(f"=== INICIO WRITE MASTER PCBEV ===")
    logger.info(f"  Destino:  {master_path}")
    logger.info(f"  Entrante: {len(df_processed):,} registros")
    logger.info(f"  Dry run:  {dry_run}")

    if not master_path.exists():
        logger.info("  master_pcbev.csv no existe — creando desde cero")
        stats["registros_nuevos"] = len(df_processed)
        stats["total_final"] = len(df_processed)
        df_final = df_processed.copy()
    else:
        logger.info(f"  Leyendo master existente: {master_path}")
        df_master = pd.read_csv(master_path, low_memory=False)
        stats["registros_anteriores"] = len(df_master)

        pk_new    = df_processed[pk_cols].apply(tuple, axis=1)
        pk_master = df_master[pk_cols].apply(tuple, axis=1)
        overlap   = pk_master.isin(set(pk_new))

        stats["registros_reemplazados"] = int(overlap.sum())
        stats["registros_nuevos"] = len(df_processed) - stats["registros_reemplazados"]

        df_keep = df_master[~overlap].copy()
        all_cols = list(dict.fromkeys(list(df_keep.columns) + list(df_processed.columns)))
        df_final = pd.concat(
            [df_keep.reindex(columns=all_cols),
             df_processed.reindex(columns=all_cols)],
            ignore_index=True,
        )
        stats["total_final"] = len(df_final)

        # Backup
        if backup and not dry_run:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = master_path.with_name(f"master_pcbev_backup_{ts}.csv")
            shutil.copy2(master_path, backup_path)
            logger.info(f"  Backup: {backup_path}")

    if not dry_run:
        df_final.to_csv(master_path, index=False, encoding="utf-8-sig")
        logger.info(f"  master_pcbev.csv escrito: {stats['total_final']:,} registros")
    else:
        stats["total_final"] = len(df_final)
        logger.info("  [DRY RUN] — no se escribió a disco")

    logger.info(
        f"=== WRITE COMPLETADO ===\n"
        f"  Anteriores:   {stats['registros_anteriores']:>6,}\n"
        f"  Reemplazados: {stats['registros_reemplazados']:>6,}\n"
        f"  Nuevos:       {stats['registros_nuevos']:>6,}\n"
        f"  TOTAL FINAL:  {stats['total_final']:>6,}"
    )
    return stats


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline PC/Bebidas Brasil · Share Dashboard"
    )
    parser.add_argument(
        "--files", nargs="+", required=True,
        help="Archivos Excel PC/Bev (ej: 02_2026.xlsx 03_2026.xlsx)"
    )
    parser.add_argument("--fx",       required=True, help="Excel tipo de cambio")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Simular sin escribir master_pcbev.csv")
    parser.add_argument("--no-backup", action="store_true",
                        help="No crear backup del master anterior")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ config.yaml no encontrado: {config_path.absolute()}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    setup_logging(config)
    logger = logging.getLogger("run_pipeline_pcbev")

    print(f"\n{'='*65}")
    print(f"  SHARE DASHBOARD — PIPELINE PC/BEBIDAS BRASIL")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'='*65}")
    if args.dry_run:
        print(f"  ⚠️  MODO DRY RUN — no se escribirá a disco")
    print(f"  Archivos: {[Path(f).name for f in args.files]}")
    print()

    sys.path.insert(0, str(Path(__file__).parent))
    from ingest_pcbev import ingest_pcbev, load_config
    from process_pcbev import process_pcbev

    start = datetime.now()

    try:
        # PASO 1 — INGEST
        logger.info("━━━ PASO 1/3: INGEST PC/BEV ━━━")
        df_raw = ingest_pcbev(args.files, config)
        print(f"  ✅ Ingest:   {len(df_raw):,} filas consolidadas")

        # PASO 2 — PROCESS
        logger.info("━━━ PASO 2/3: PROCESS PC/BEV ━━━")
        df_processed, report = process_pcbev(df_raw, config, fx_path=args.fx)
        print(f"  ✅ Process:  {len(df_processed):,} registros válidos | "
              f"{len(report.rejected):,} rechazados")

        # PASO 3 — WRITE MASTER
        logger.info("━━━ PASO 3/3: WRITE MASTER PCBEV ━━━")
        master_path = Path(config["pcbev_file"]["master"])
        stats = write_master_pcbev(
            df_processed,
            master_path,
            backup=not args.no_backup,
            dry_run=args.dry_run,
        )

        elapsed = (datetime.now() - start).total_seconds()

        print(f"\n{'─'*65}")
        print(f"  PIPELINE PC/BEV COMPLETADO EN {elapsed:.1f}s")
        print(f"{'─'*65}")
        print(f"  Archivos procesados:  {len(args.files)}")
        print(f"  Filas raw:            {len(df_raw):,}")
        print(f"  Registros válidos:    {len(df_processed):,}")
        print(f"  Rechazados:           {len(report.rejected):,}")
        print(f"  Total en master:      {stats['total_final']:,}")
        print(f"  GLI en master:        {df_processed['es_genomma'].sum():,}")

        # Resumen GLI
        gli = df_processed[df_processed["es_genomma"]]
        if len(gli) > 0:
            print(f"\n  GLI por categoría (AM1):")
            by_cat = gli.groupby("categoria")["mkt_am1"].sum()
            for cat, val in by_cat.items():
                usd = val / df_processed[df_processed["categoria"] == cat]["er_aplicado"].iloc[0]
                print(f"    {cat:<15} {val/1e6:>8.1f}M BRL  |  {usd/1e6:>6.1f}M USD")

        print(f"{'='*65}\n")

    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        logger.exception(f"PIPELINE PC/BEV FALLIDO después de {elapsed:.1f}s")
        print(f"\n❌ PIPELINE FALLIDO: {type(e).__name__}")
        print(f"   {str(e)[:500]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
