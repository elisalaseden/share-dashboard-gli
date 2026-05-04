"""
run_pipeline.py — Orquestador del pipeline completo
Share Dashboard · Investor Reporting Pipeline

Punto de entrada único para ejecución mensual.
Encadena: ingest → process → update_master → log final.

Uso:
    python scripts/run_pipeline.py --shares ruta/shares.xlsx --fx ruta/fx.xlsx
    python scripts/run_pipeline.py --shares ruta/shares.xlsx --fx ruta/fx.xlsx --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')


def setup_logging(config: dict) -> None:
    log_dir = Path(config["paths"].get("log_dir", "logs/"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

    logging.basicConfig(
        level=logging.INFO,
        format=config["logging"]["format"],
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.info(f"Log activo: {log_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline completo Share Dashboard · Investor Reporting"
    )
    parser.add_argument("--shares",  required=True, help="Ruta al Excel de shares (mensual)")
    parser.add_argument("--fx",      required=True, help="Ruta al Excel tipo de cambio")
    parser.add_argument("--config",  default="config.yaml", help="Ruta a config.yaml")
    parser.add_argument("--sheets",  nargs="+", default=None,
                        help="Hojas específicas (ej: --sheets ARG MEX). Default: todas")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simular sin escribir master.csv")
    parser.add_argument("--no-backup", action="store_true",
                        help="No crear backup del master anterior")
    args = parser.parse_args()

    # Cargar config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ config.yaml no encontrado: {config_path.absolute()}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    setup_logging(config)
    logger = logging.getLogger("run_pipeline")

    print(f"\n{'='*65}")
    print(f"  SHARE DASHBOARD — PIPELINE MENSUAL")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'='*65}")
    if args.dry_run:
        print(f"  ⚠️  MODO DRY RUN — no se escribirá a disco")
    print()

    # Importar módulos del pipeline
    sys.path.insert(0, str(Path(__file__).parent))
    from ingest import ingest_shares_file, load_config
    from process import process
    from update_master import update_master

    start = datetime.now()

    try:
        # PASO 1 — INGEST
        logger.info("━━━ PASO 1/3: INGEST ━━━")
        raw_data = ingest_shares_file(
            filepath=args.shares,
            config=config,
            sheets_override=args.sheets,
        )
        total_raw = sum(len(df) for df in raw_data.values())
        print(f"  ✅ Ingest:  {len(raw_data)} países | {total_raw:,} filas raw")

        # PASO 2 — PROCESS
        logger.info("━━━ PASO 2/3: PROCESS ━━━")
        df_processed, validation_report = process(
            raw_data=raw_data,
            config=config,
            fx_path=args.fx,
        )
        print(f"  ✅ Process: {len(df_processed):,} registros válidos | "
              f"{len(validation_report.rejected):,} rechazados")

        # PASO 3 — UPDATE MASTER
        logger.info("━━━ PASO 3/3: UPDATE MASTER ━━━")
        stats = update_master(
            df_processed=df_processed,
            config=config,
            backup=not args.no_backup,
            dry_run=args.dry_run,
        )

        elapsed = (datetime.now() - start).total_seconds()

        print(f"\n{'─'*65}")
        print(f"  PIPELINE COMPLETADO EN {elapsed:.1f}s")
        print(f"{'─'*65}")
        print(f"  Países procesados:      {len(raw_data)}")
        print(f"  Filas raw:              {total_raw:,}")
        print(f"  Registros válidos:      {len(df_processed):,}")
        print(f"  Registros rechazados:   {len(validation_report.rejected):,}")
        print(f"  Total en master.csv:    {stats['total_final']:,}")
        print(f"{'='*65}\n")

        # Si hay rechazos, reportarlos
        if validation_report.rejected:
            print(f"  ⚠️  REGISTROS RECHAZADOS ({len(validation_report.rejected)}):")
            for r in validation_report.rejected[:10]:  # máximo 10 en consola
                print(f"     [{r.get('pais','?')}] {r.get('laboratorio','?')} | "
                      f"{r.get('producto','?')} → {r.get('reason','')}")
            if len(validation_report.rejected) > 10:
                print(f"     ... y {len(validation_report.rejected) - 10} más. Ver log completo.")
            print()

    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        logger.exception(f"PIPELINE FALLIDO después de {elapsed:.1f}s")
        print(f"\n❌ PIPELINE FALLIDO: {type(e).__name__}")
        print(f"   {str(e)[:500]}")
        print(f"\n   Ver log completo para stack trace completo.")
        sys.exit(1)


if __name__ == "__main__":
    main()
