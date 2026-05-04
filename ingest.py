"""
ingest.py — Lectura y detección de layout
Share Dashboard · Investor Reporting Pipeline

Responsabilidad ÚNICA: leer cada hoja del Excel de shares,
validar el fingerprint y retornar DataFrames raw sin transformar.

NO normaliza. NO escribe a disco. NO lee tipo de cambio.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EXCEPCIONES PROPIAS — nunca silenciosas
# ---------------------------------------------------------------------------

class LayoutNotRecognizedError(Exception):
    """El archivo no coincide con el fingerprint definido en config.yaml."""
    pass


class MissingSheetError(Exception):
    """Una hoja esperada no existe en el archivo."""
    pass


class MissingColumnsError(Exception):
    """Columnas requeridas ausentes en la hoja."""
    pass


# ---------------------------------------------------------------------------
# CARGA DE CONFIG
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> dict:
    """
    Carga config.yaml desde disco.

    Args:
        config_path: ruta al archivo de configuración.
    Returns:
        dict con la configuración completa.
    Raises:
        FileNotFoundError: si el archivo no existe.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"config.yaml no encontrado en: {path.absolute()}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# VALIDACIÓN DE FINGERPRINT
# ---------------------------------------------------------------------------

def _validate_fingerprint(df: pd.DataFrame, expected_cols: list[str], sheet_name: str) -> None:
    """
    Verifica que el DataFrame tenga exactamente las columnas esperadas.

    Args:
        df:            DataFrame crudo leído del Excel.
        expected_cols: lista de nombres de columna definidos en config.
        sheet_name:    nombre de la hoja (para mensajes de error).
    Raises:
        LayoutNotRecognizedError: si hay columnas faltantes.
    """
    actual_cols = set(df.columns.tolist())
    expected_set = set(expected_cols)
    missing = expected_set - actual_cols

    if missing:
        raise LayoutNotRecognizedError(
            f"Hoja '{sheet_name}': columnas faltantes → {sorted(missing)}\n"
            f"Columnas presentes: {sorted(actual_cols)}\n"
            f"Verifica que el layout del archivo coincide con config.yaml."
        )

    logger.debug(f"Hoja '{sheet_name}': fingerprint OK — {len(actual_cols)} columnas detectadas")


# ---------------------------------------------------------------------------
# LECTURA DE UNA HOJA
# ---------------------------------------------------------------------------

def read_sheet(
    filepath: str,
    sheet_name: str,
    cfg_shares: dict,
) -> pd.DataFrame:
    """
    Lee una hoja del Excel de shares y valida el fingerprint.

    Args:
        filepath:   ruta al archivo Excel.
        sheet_name: nombre de la hoja (ej: 'ARG').
        cfg_shares: sección 'shares_file' del config.
    Returns:
        DataFrame con la columna 'pais' agregada (código de hoja).
    Raises:
        MissingSheetError:          si la hoja no existe en el archivo.
        LayoutNotRecognizedError:   si el layout no coincide con el fingerprint.
        MissingColumnsError:        si MetricName tiene valores inesperados.
    """
    logger.info(f"Leyendo hoja: {sheet_name} | archivo: {filepath}")

    try:
        df = pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=cfg_shares["header_row"],
            engine="openpyxl",
        )
    except Exception as e:
        if "Worksheet" in str(e) or "not found" in str(e).lower():
            raise MissingSheetError(
                f"Hoja '{sheet_name}' no encontrada en el archivo '{filepath}'.\n"
                f"Verifica que el archivo corresponde al período correcto."
            ) from e
        raise

    # Eliminar filas completamente vacías
    df = df.dropna(how="all").reset_index(drop=True)

    # Columnas esperadas desde config
    cols_cfg = cfg_shares["columns"]
    expected_cols = list(cols_cfg.values())
    _validate_fingerprint(df, expected_cols, sheet_name)

    # Validar valores de MetricName
    metric_cfg = cfg_shares["metric_values"]
    valid_metrics = set(metric_cfg.values())
    actual_metrics = set(df[cols_cfg["metric_name"]].dropna().unique())
    unexpected_metrics = actual_metrics - valid_metrics

    if unexpected_metrics:
        logger.warning(
            f"Hoja '{sheet_name}': valores inesperados en MetricName → {unexpected_metrics}. "
            f"Estas filas serán ignoradas en process.py."
        )

    # Agregar columna país
    df["pais"] = sheet_name

    logger.info(
        f"Hoja '{sheet_name}': {len(df):,} filas leídas | "
        f"métricas: {sorted(actual_metrics)}"
    )
    return df


# ---------------------------------------------------------------------------
# LECTURA COMPLETA DEL ARCHIVO
# ---------------------------------------------------------------------------

def ingest_shares_file(
    filepath: str,
    config: dict,
    sheets_override: Optional[list[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Lee todas las hojas del Excel de shares definidas en config.

    Args:
        filepath:        ruta al archivo Excel.
        config:          configuración completa cargada de config.yaml.
        sheets_override: lista opcional de hojas a leer (para pruebas parciales).
    Returns:
        Dict[código_país → DataFrame crudo con columna 'pais'].
    Raises:
        FileNotFoundError:        si el archivo no existe.
        MissingSheetError:        si alguna hoja esperada no está en el archivo.
        LayoutNotRecognizedError: si alguna hoja no pasa el fingerprint.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Archivo de shares no encontrado: {path.absolute()}")

    cfg_shares = config["shares_file"]
    sheets_cfg = cfg_shares["sheets"]         # {ARG: Argentina, BRA: Brasil, ...}
    target_sheets = sheets_override or list(sheets_cfg.keys())

    logger.info(f"Iniciando ingesta | archivo: {filepath} | hojas: {target_sheets}")

    results: Dict[str, pd.DataFrame] = {}
    errors: list[str] = []

    for sheet_code in target_sheets:
        try:
            df = read_sheet(filepath, sheet_code, cfg_shares)
            results[sheet_code] = df
        except (MissingSheetError, LayoutNotRecognizedError, MissingColumnsError) as e:
            error_msg = f"[{sheet_code}] {type(e).__name__}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

    if errors:
        # Siempre fallar loud — nunca continuar silencioso
        raise LayoutNotRecognizedError(
            f"Ingesta completada con {len(errors)} error(es):\n" +
            "\n".join(errors)
        )

    total_rows = sum(len(df) for df in results.values())
    logger.info(
        f"Ingesta completada | {len(results)} hojas | {total_rows:,} filas totales"
    )
    return results


# ---------------------------------------------------------------------------
# SMOKE TEST — ejecutar con: python scripts/ingest.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smoke test de ingest.py")
    parser.add_argument("--file", required=True, help="Ruta al Excel de shares")
    parser.add_argument("--config", default="config.yaml", help="Ruta a config.yaml")
    parser.add_argument(
        "--sheets", nargs="+", default=None,
        help="Hojas específicas a probar (ej: --sheets ARG MEX)"
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SMOKE TEST — ingest.py | {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    try:
        config = load_config(args.config)
        results = ingest_shares_file(args.file, config, sheets_override=args.sheets)

        print(f"\n✅ INGESTA EXITOSA")
        print(f"{'─'*40}")
        for pais, df in results.items():
            print(f"  {pais}: {len(df):>6,} filas | columnas: {list(df.columns)}")

        print(f"\n{'─'*40}")
        print(f"  TOTAL: {sum(len(df) for df in results.values()):,} filas")
        print(f"\nOutput esperado en producción: pasar results a process.py")

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(str(e))
        sys.exit(1)
