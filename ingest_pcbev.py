"""
ingest_pcbev.py — Lectura y normalización de archivos PC/Bebidas Brasil
Share Dashboard · Investor Reporting Pipeline

Responsabilidad ÚNICA: leer uno o varios archivos Excel de PC/Bebidas,
normalizar fingerprint por variante, combinar por regla max_rows_per_period
y retornar un DataFrame raw consolidado sin transformaciones de negocio.

NO calcula AM. NO calcula MS%. NO lee tipo de cambio. NO escribe a disco.
"""

import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EXCEPCIONES PROPIAS
# ---------------------------------------------------------------------------

class PCBevLayoutError(Exception):
    """Fingerprint no reconocido tras aplicar alias."""
    pass


class PCBevMissingColumnsError(Exception):
    """Columnas requeridas ausentes tras normalización."""
    pass


# ---------------------------------------------------------------------------
# CARGA DE CONFIG
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> dict:
    """
    Carga config.yaml.

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
# DETECCIÓN DE HOJA
# ---------------------------------------------------------------------------

def _detect_sheet(filepath: str, sheet_aliases: List[str]) -> str:
    """
    Detecta el nombre de la hoja disponible en el archivo,
    probando cada alias en orden.

    Args:
        filepath:      ruta al archivo Excel.
        sheet_aliases: lista de nombres de hoja aceptados (config: sheet_aliases).
    Returns:
        Nombre de hoja encontrado.
    Raises:
        PCBevLayoutError: si ningún alias coincide con las hojas del archivo.
    """
    xl = pd.ExcelFile(filepath, engine="openpyxl")
    available = xl.sheet_names
    for alias in sheet_aliases:
        if alias in available:
            logger.debug(f"  Hoja detectada: '{alias}' en {Path(filepath).name}")
            return alias
    raise PCBevLayoutError(
        f"Ninguna hoja reconocida en '{filepath}'.\n"
        f"  Hojas disponibles: {available}\n"
        f"  Hojas esperadas (config sheet_aliases): {sheet_aliases}"
    )


# ---------------------------------------------------------------------------
# NORMALIZACIÓN DE COLUMNAS
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame, column_aliases: Dict[str, str]) -> pd.DataFrame:
    """
    Renombra columnas usando el mapa de alias definido en config.
    Solo renombra columnas que existen — ignora alias sin match.

    Args:
        df:             DataFrame crudo.
        column_aliases: dict {nombre_variante → nombre_canónico}.
    Returns:
        DataFrame con columnas renombradas.
    """
    rename_map = {k: v for k, v in column_aliases.items() if k in df.columns}
    if rename_map:
        logger.debug(f"  Columnas renombradas: {rename_map}")
        df = df.rename(columns=rename_map)
    return df


# ---------------------------------------------------------------------------
# VALIDACIÓN DE FINGERPRINT
# ---------------------------------------------------------------------------

def _validate_fingerprint(
    df: pd.DataFrame,
    required_cols: List[str],
    filepath: str,
) -> None:
    """
    Verifica que el DataFrame tenga las columnas canónicas requeridas.

    Args:
        df:            DataFrame post-normalización.
        required_cols: lista de columnas canónicas (valores de cfg['columns']).
        filepath:      nombre del archivo (para mensajes de error).
    Raises:
        PCBevMissingColumnsError: si faltan columnas tras normalización.
    """
    present = set(df.columns)
    missing = set(required_cols) - present
    if missing:
        raise PCBevMissingColumnsError(
            f"Columnas faltantes en '{Path(filepath).name}' tras normalización: {sorted(missing)}\n"
            f"  Columnas presentes: {sorted(present)}\n"
            f"  Agrega el alias correspondiente en config.yaml → pcbev_file → column_aliases."
        )
    logger.debug(f"  Fingerprint OK: {len(present)} columnas presentes")


# ---------------------------------------------------------------------------
# NORMALIZACIÓN DE VALORES SUBMARKET
# ---------------------------------------------------------------------------

def _normalize_submarket_values(
    df: pd.DataFrame,
    submarket_col: str,
    submarket_aliases: Dict[str, str],
) -> pd.DataFrame:
    """
    Normaliza valores de la columna Submarket al nombre canónico.

    Args:
        df:                DataFrame post-fingerprint.
        submarket_col:     nombre canónico de la columna submarket.
        submarket_aliases: dict {valor_variante → valor_canónico}.
    Returns:
        DataFrame con valores de submarket normalizados.
    """
    if submarket_col not in df.columns:
        return df
    df = df.copy()
    original_uniques = set(df[submarket_col].dropna().unique())
    df[submarket_col] = df[submarket_col].replace(submarket_aliases)
    normalized_uniques = set(df[submarket_col].dropna().unique())
    changed = original_uniques - normalized_uniques
    if changed:
        logger.debug(f"  Submarket normalizado: {changed}")
    return df


# ---------------------------------------------------------------------------
# EXTRACCIÓN DE FECHA DE REPORTE DEL NOMBRE DE ARCHIVO
# ---------------------------------------------------------------------------

def _parse_report_date(filepath: str) -> Optional[pd.Timestamp]:
    """
    Extrae la fecha de reporte del nombre de archivo con patrón MM_YYYY.
    Ejemplo: '03_2026_Brasil_PC_Beverages_Base_Shares.xlsx' → 2026-03-01.

    Args:
        filepath: ruta al archivo.
    Returns:
        Timestamp de la fecha de reporte, o None si no se puede parsear.
    """
    name = Path(filepath).stem
    match = re.match(r"^(\d{2})_(\d{4})", name)
    if match:
        mes, anio = int(match.group(1)), int(match.group(2))
        return pd.Timestamp(year=anio, month=mes, day=1)
    logger.warning(f"No se pudo extraer fecha de reporte del nombre: '{name}'")
    return None


# ---------------------------------------------------------------------------
# LECTURA DE UN ARCHIVO
# ---------------------------------------------------------------------------

def read_pcbev_file(filepath: str, cfg_pcbev: dict) -> pd.DataFrame:
    """
    Lee un archivo Excel PC/Bev, detecta la hoja, normaliza columnas
    y valores de submarket, y retorna DataFrame raw.

    Args:
        filepath:   ruta al archivo Excel.
        cfg_pcbev:  sección 'pcbev_file' del config.
    Returns:
        DataFrame raw con columnas canónicas + columna '_report_date'.
    Raises:
        FileNotFoundError:      si el archivo no existe.
        PCBevLayoutError:       si la hoja no se reconoce.
        PCBevMissingColumnsError: si faltan columnas tras normalización.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Archivo PC/Bev no encontrado: {path.absolute()}")

    logger.info(f"Leyendo archivo: {path.name}")

    # Detectar hoja
    sheet = _detect_sheet(filepath, cfg_pcbev["sheet_aliases"])

    # Leer crudo
    df = pd.read_excel(
        filepath,
        sheet_name=sheet,
        header=cfg_pcbev["header_row"],
        engine="openpyxl",
        dtype=str,          # leer todo como string primero — parsear luego
    )
    df = df.dropna(how="all").reset_index(drop=True)
    logger.info(f"  Filas leídas: {len(df):,} | hoja: '{sheet}'")

    # Normalizar columnas
    df = _normalize_columns(df, cfg_pcbev["column_aliases"])

    # Validar fingerprint
    required_cols = list(cfg_pcbev["columns"].values())
    _validate_fingerprint(df, required_cols, filepath)

    # Normalizar valores submarket
    submarket_col = cfg_pcbev["columns"]["submarket"]
    df = _normalize_submarket_values(df, submarket_col, cfg_pcbev["submarket_aliases"])

    # Parsear tipos numéricos
    for num_col in [
        cfg_pcbev["columns"]["unidades"],
        cfg_pcbev["columns"]["valor_desconto"],
        cfg_pcbev["columns"]["valor_consumidor"],
    ]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    # Agregar fecha de reporte como metadata (para regla de combinación)
    report_date = _parse_report_date(filepath)
    df["_report_date"] = report_date
    df["_source_file"] = path.name

    logger.info(
        f"  Archivo procesado: {path.name} | "
        f"reporte: {report_date.strftime('%m/%Y') if report_date else 'desconocido'} | "
        f"submarket únicos: {sorted(df[submarket_col].dropna().unique())}"
    )
    return df


# ---------------------------------------------------------------------------
# COMBINACIÓN DE ARCHIVOS — REGLA max_rows_per_period
# ---------------------------------------------------------------------------

def _combine_files(
    frames: List[pd.DataFrame],
    periodo_col: str,
) -> pd.DataFrame:
    """
    Combina múltiples DataFrames aplicando la regla max_rows_per_period:
    para cada período MM/YYYY, prevalece el archivo con mayor número de filas.

    Esto garantiza que períodos parciales (borde de reporte) no sobreescriban
    datos completos del archivo anterior.

    Args:
        frames:      lista de DataFrames leídos de cada archivo.
        periodo_col: nombre canónico de la columna PERIODO.
    Returns:
        DataFrame consolidado sin duplicados de período.
    """
    if len(frames) == 1:
        return frames[0].drop(columns=["_report_date", "_source_file"], errors="ignore")

    # Contar filas por período por archivo
    period_winner: Dict[str, pd.DataFrame] = {}   # periodo → DataFrame filtrado a ese periodo

    all_periods: set = set()
    for df in frames:
        all_periods.update(df[periodo_col].dropna().unique())

    logger.info(f"Combinando {len(frames)} archivo(s) | {len(all_periods)} períodos únicos total")

    for period in sorted(all_periods):
        best_df = None
        best_rows = -1
        best_source = ""
        for df in frames:
            subset = df[df[periodo_col] == period]
            n = len(subset)
            if n > best_rows:
                best_rows = n
                best_df = subset
                best_source = df["_source_file"].iloc[0] if "_source_file" in df.columns and len(subset) > 0 else "?"
        if best_df is not None:
            period_winner[period] = best_df
            logger.debug(f"  Período {period}: {best_rows:,} filas → {best_source}")

    combined = pd.concat(list(period_winner.values()), ignore_index=True)
    combined = combined.drop(columns=["_report_date", "_source_file"], errors="ignore")

    logger.info(f"Combinación completada: {len(combined):,} filas totales")
    return combined


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def ingest_pcbev(
    filepaths: List[str],
    config: dict,
) -> pd.DataFrame:
    """
    Lee y consolida uno o varios archivos Excel de PC/Bebidas Brasil.

    Flujo:
        1. Lee cada archivo → normaliza columnas y submarket values.
        2. Combina usando regla max_rows_per_period.
        3. Agrega columna 'pais' desde config.
        4. Retorna DataFrame raw consolidado listo para process_pcbev.py.

    Args:
        filepaths: lista de rutas a archivos Excel PC/Bev (orden no importa).
        config:    configuración completa cargada de config.yaml.
    Returns:
        DataFrame consolidado con columnas canónicas + 'pais'.
    Raises:
        ValueError:              si filepaths está vacío.
        FileNotFoundError:       si algún archivo no existe.
        PCBevLayoutError:        si algún archivo no pasa fingerprint.
        PCBevMissingColumnsError: si faltan columnas tras normalización.
    """
    if not filepaths:
        raise ValueError("filepaths está vacío — proporciona al menos un archivo PC/Bev.")

    cfg_pcbev = config["pcbev_file"]
    periodo_col = cfg_pcbev["columns"]["periodo"]

    logger.info(f"=== INICIO INGEST PC/BEV | {len(filepaths)} archivo(s) ===")

    frames: List[pd.DataFrame] = []
    errors: List[str] = []

    for fp in filepaths:
        try:
            df = read_pcbev_file(fp, cfg_pcbev)
            frames.append(df)
        except (FileNotFoundError, PCBevLayoutError, PCBevMissingColumnsError) as e:
            msg = f"[{Path(fp).name}] {type(e).__name__}: {e}"
            logger.error(msg)
            errors.append(msg)

    if errors:
        raise PCBevLayoutError(
            f"Ingest PC/Bev completado con {len(errors)} error(es):\n" +
            "\n".join(errors)
        )

    # Combinar
    df_combined = _combine_files(frames, periodo_col)

    # Agregar país
    df_combined["pais"] = cfg_pcbev["pais"]

    # Log de períodos finales
    df_combined["_fecha_periodo"] = pd.to_datetime(
        df_combined[periodo_col], format="%m/%Y"
    )
    min_per = df_combined["_fecha_periodo"].min().strftime("%m/%Y")
    max_per = df_combined["_fecha_periodo"].max().strftime("%m/%Y")
    n_periodos = df_combined["_fecha_periodo"].nunique()
    df_combined = df_combined.drop(columns=["_fecha_periodo"])

    logger.info(
        f"=== INGEST PC/BEV COMPLETADO ===\n"
        f"  Archivos procesados: {len(frames)}\n"
        f"  Período: {min_per} → {max_per} ({n_periodos} meses)\n"
        f"  Filas consolidadas: {len(df_combined):,}"
    )
    return df_combined


# ---------------------------------------------------------------------------
# SMOKE TEST — ejecutar con: python scripts/ingest_pcbev.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smoke test de ingest_pcbev.py")
    parser.add_argument(
        "--files", nargs="+", required=True,
        help="Rutas a los archivos Excel PC/Bev (ej: --files 02_2026.xlsx 03_2026.xlsx)"
    )
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SMOKE TEST — ingest_pcbev.py | {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    try:
        config = load_config(args.config)
        df = ingest_pcbev(args.files, config)

        cfg_pcbev = config["pcbev_file"]
        periodo_col = cfg_pcbev["columns"]["periodo"]
        submarket_col = cfg_pcbev["columns"]["submarket"]
        market_col = cfg_pcbev["columns"]["market"]
        valor_col = cfg_pcbev["valor_col"]

        # Parsear fechas para stats
        df["_fecha"] = pd.to_datetime(df[periodo_col], format="%m/%Y")

        print(f"\n✅ INGEST EXITOSO")
        print(f"{'─'*40}")
        print(f"  Filas totales:     {len(df):,}")
        print(f"  Período mín:       {df['_fecha'].min().strftime('%m/%Y')}")
        print(f"  Período máx:       {df['_fecha'].max().strftime('%m/%Y')}")
        print(f"  Meses cubiertos:   {df['_fecha'].nunique()}")
        print(f"  Market únicos:     {sorted(df[market_col].dropna().unique())}")
        print(f"  Submarket únicos:  {sorted(df[submarket_col].dropna().unique())}")
        print(f"  Negativos {valor_col}: {(df[valor_col] < 0).sum():,}")
        print(f"\n  Muestra (3 filas):")
        print(df.drop(columns=["_fecha"]).head(3).to_string())

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(str(e))
        sys.exit(1)
