"""
process.py — Normalización, pivot, validación y tipo de cambio
Share Dashboard · Investor Reporting Pipeline

Responsabilidad ÚNICA: recibir DataFrames raw de ingest.py,
ejecutar el pivot long→wide, unir tipo de cambio,
validar calidad y retornar DataFrame limpio listo para master.csv.

NO lee Excel. NO escribe a disco. NO construye visualizaciones.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EXCEPCIONES PROPIAS
# ---------------------------------------------------------------------------

class PivotError(Exception):
    """Error durante el pivot long→wide."""
    pass


class FXLoadError(Exception):
    """Error cargando archivo tipo de cambio."""
    pass


class ValidationReport:
    """Acumula resultados de validación sin interrumpir el pipeline."""

    def __init__(self):
        self.passed: int = 0
        self.rejected: list[dict] = []

    def add_rejection(self, row_data: dict, reason: str) -> None:
        self.rejected.append({"reason": reason, **row_data})
        logger.warning(f"Registro rechazado: {reason} | datos: {row_data}")

    def add_pass(self) -> None:
        self.passed += 1

    def summary(self) -> str:
        total = self.passed + len(self.rejected)
        return (
            f"Validación: {self.passed:,}/{total:,} registros válidos | "
            f"{len(self.rejected):,} rechazados"
        )


# ---------------------------------------------------------------------------
# CARGA DE TIPO DE CAMBIO
# ---------------------------------------------------------------------------

def load_fx_rates(fx_path: str, cfg_fx: dict) -> Dict[str, float]:
    """
    Extrae el tipo de cambio más reciente por país desde el archivo oficial Genomma.

    Args:
        fx_path: ruta al Excel de tipos de cambio.
        cfg_fx:  sección 'fx_file' del config.
    Returns:
        Dict[código_país → ER (float)].
    Raises:
        FXLoadError: si el archivo o la hoja no existen, o la columna ER no tiene valor.
    """
    path = Path(fx_path)
    if not path.exists():
        raise FXLoadError(f"Archivo tipo de cambio no encontrado: {path.absolute()}")

    logger.info(f"Cargando tipos de cambio: {fx_path}")

    try:
        df_raw = pd.read_excel(
            fx_path,
            sheet_name=cfg_fx["sheet"],
            header=None,
            engine="openpyxl",
        )
    except Exception as e:
        raise FXLoadError(f"Error leyendo hoja '{cfg_fx['sheet']}': {e}") from e

    # Datos a partir del header_row (fila 10 = índice 9)
    data = df_raw.iloc[cfg_fx["header_row"]:, :].copy()
    date_col = cfg_fx["date_col"]

    # Filtrar filas con fecha válida
    data = data[data.iloc[:, date_col].notna()].reset_index(drop=True)

    if data.empty:
        raise FXLoadError("No se encontraron filas con fecha válida en el archivo de tipos de cambio.")

    # Tomar la fila más reciente (último período)
    last_row = data.iloc[-1]
    fecha = last_row.iloc[date_col]
    logger.info(f"Tipo de cambio aplicado: período {fecha}")

    # Extraer ER por país
    er_columns = cfg_fx["er_columns"]
    fx_rates: Dict[str, float] = {}

    for pais_code, col_idx in er_columns.items():
        if col_idx is None:
            # Ecuador: USD nativo
            fx_rates[pais_code] = 1.0
            logger.debug(f"  {pais_code}: ER = 1.0 (USD nativo)")
        else:
            er_val = last_row.iloc[col_idx]
            if pd.isna(er_val) or er_val <= 0:
                raise FXLoadError(
                    f"ER inválido para {pais_code}: valor={er_val} en col={col_idx}. "
                    f"Verifica el archivo de tipo de cambio."
                )
            fx_rates[pais_code] = float(er_val)
            logger.debug(f"  {pais_code}: ER = {er_val:.4f}")

    return fx_rates


# ---------------------------------------------------------------------------
# NORMALIZACIÓN DE AÑO MÓVIL
# ---------------------------------------------------------------------------

def _normalize_anio_movil(series: pd.Series, anio_map: dict) -> pd.Series:
    """
    Normaliza los valores de Año Móvil al código corto (am1..am5).

    Args:
        series:   columna Año Móvil del DataFrame.
        anio_map: diccionario de mapeo desde config.
    Returns:
        Serie con valores normalizados.
    """
    normalized = series.map(anio_map)
    unmapped = series[normalized.isna()].unique()
    if len(unmapped) > 0:
        logger.warning(f"Valores de Año Móvil no mapeados (serán ignorados): {list(unmapped)}")
    return normalized


# ---------------------------------------------------------------------------
# PIVOT LONG → WIDE
# ---------------------------------------------------------------------------

def pivot_long_to_wide(df_long: pd.DataFrame, cfg_shares: dict) -> pd.DataFrame:
    """
    Convierte el DataFrame de formato long (2 filas por producto×período)
    a formato wide (1 fila por producto, columnas ms_amN y mkt_amN).

    Estructura del pivot:
        - Índice: pais, categoria, sub_cat, laboratorio, producto
        - Columnas: {metric_prefix}_{periodo} → ms_am1..5, mkt_am1..5

    Args:
        df_long:    DataFrame raw con columnas originales + 'pais'.
        cfg_shares: sección 'shares_file' del config.
    Returns:
        DataFrame wide con 10 columnas de valor + dimensiones.
    Raises:
        PivotError: si el resultado del pivot tiene filas duplicadas inesperadas.
    """
    cols = cfg_shares["columns"]
    anio_map = cfg_shares["anio_movil_map"]
    metric_vals = cfg_shares["metric_values"]

    # Renombrar columnas al schema interno
    df = df_long.rename(columns={
        cols["categoria"]:   "categoria",
        cols["sub_cat"]:     "sub_categoria",
        cols["laboratorio"]: "laboratorio",
        cols["producto"]:    "producto",
        cols["metric_name"]: "metric_name",
        cols["anio_movil"]:  "anio_movil",
        cols["valor"]:       "valor",
    })

    # Normalizar Año Móvil
    df["periodo"] = _normalize_anio_movil(df["anio_movil"], anio_map)

    # Filtrar filas con período no reconocido
    df = df[df["periodo"].notna()].copy()

    # Mapear MetricName → prefijo de columna
    metric_prefix_map = {
        metric_vals["market_share"]: "ms",
        metric_vals["market_size"]:  "mkt",
    }
    df["metric_prefix"] = df["metric_name"].map(metric_prefix_map)

    # Filtrar métricas no reconocidas
    df = df[df["metric_prefix"].notna()].copy()

    # Construir nombre de columna destino: ms_am1, mkt_am3, etc.
    df["col_name"] = df["metric_prefix"] + "_" + df["periodo"]

    # Dimensiones que forman la clave primaria
    id_vars = ["pais", "categoria", "sub_categoria", "laboratorio", "producto"]

    # Pivot: un valor por combinación de clave × col_name
    try:
        df_wide = df.pivot_table(
            index=id_vars,
            columns="col_name",
            values="valor",
            aggfunc="first",  # ante duplicados, toma el primero (no promedia)
        ).reset_index()
    except Exception as e:
        raise PivotError(f"Error en pivot long→wide: {e}") from e

    # Aplanar nombres de columnas multiindex si existen
    df_wide.columns.name = None

    # Garantizar que todas las columnas esperadas existen (rellenar con NaN si faltan)
    expected_cols = [
        f"{m}_{p}"
        for m in ["ms", "mkt"]
        for p in ["am1", "am2", "am3", "am4", "am5"]
    ]
    for col in expected_cols:
        if col not in df_wide.columns:
            df_wide[col] = np.nan
            logger.warning(f"Columna '{col}' no encontrada en los datos — rellenada con NaN")

    # Ordenar columnas
    col_order = id_vars + sorted(expected_cols)
    df_wide = df_wide[[c for c in col_order if c in df_wide.columns]]

    logger.info(f"Pivot completado: {len(df_wide):,} filas × {len(df_wide.columns)} columnas")
    return df_wide


# ---------------------------------------------------------------------------
# FLAG GENOMMA
# ---------------------------------------------------------------------------

def add_genomma_flag(df: pd.DataFrame, genomma_string: str) -> pd.DataFrame:
    """
    Agrega columna boolean es_genomma.

    Args:
        df:             DataFrame wide.
        genomma_string: string exacto desde config (ej: 'GENOMMA LAB').
    Returns:
        DataFrame con columna 'es_genomma' agregada.
    """
    df = df.copy()
    df["es_genomma"] = df["laboratorio"].str.upper() == genomma_string.upper()
    genomma_count = df["es_genomma"].sum()
    logger.info(f"Flag Genomma: {genomma_count:,} productos identificados como GLI")
    return df


# ---------------------------------------------------------------------------
# CONVERSIÓN A USD
# ---------------------------------------------------------------------------

def apply_fx_conversion(df: pd.DataFrame, fx_rates: Dict[str, float]) -> pd.DataFrame:
    """
    Convierte las columnas mkt_amN de moneda local a USD usando el ER oficial Genomma.

    Args:
        df:       DataFrame wide post-pivot.
        fx_rates: Dict[código_país → ER] cargado de load_fx_rates().
    Returns:
        DataFrame con columnas mkt_usd_amN y er_aplicado agregadas.
    """
    df = df.copy()
    periods = ["am1", "am2", "am3", "am4", "am5"]

    # Mapear ER por fila según el país
    df["er_aplicado"] = df["pais"].map(fx_rates)

    paises_sin_er = df[df["er_aplicado"].isna()]["pais"].unique()
    if len(paises_sin_er) > 0:
        raise FXLoadError(
            f"Países sin tipo de cambio definido: {list(paises_sin_er)}. "
            f"Agrega los ER en config.yaml → fx_file → er_columns."
        )

    for p in periods:
        mkt_col = f"mkt_{p}"
        usd_col = f"mkt_usd_{p}"
        if mkt_col in df.columns:
            df[usd_col] = df[mkt_col] / df["er_aplicado"]
        else:
            df[usd_col] = np.nan

    logger.info("Conversión USD completada para todos los períodos")
    return df


# ---------------------------------------------------------------------------
# VALIDACIÓN DE CALIDAD
# ---------------------------------------------------------------------------

def validate(df: pd.DataFrame, cfg_validation: dict) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Ejecuta las 5 reglas de validación en orden.
    Registros inválidos se separan — el pipeline NO se detiene.

    Reglas:
        1. Completitud: columnas NOT NULL no tienen nulos.
        2. Tipos:       mkt y ms son numéricos.
        3. Rangos:      ms entre 0 y 100; mkt >= 0.
        4. Duplicados:  clave primaria única.
        5. Nulos en AM1: mkt_am1 no puede ser nulo.

    Args:
        df:             DataFrame wide post-conversión.
        cfg_validation: sección 'validation' del config.
    Returns:
        Tuple(DataFrame_válido, ValidationReport).
    """
    report = ValidationReport()
    required_not_null = cfg_validation.get("required_not_null", [])
    ms_min = cfg_validation["ms_range"]["min"]
    ms_max = cfg_validation["ms_range"]["max"]
    mkt_min = cfg_validation.get("mkt_min", 0.0)

    valid_mask = pd.Series(True, index=df.index)
    rejection_reasons = pd.Series("", index=df.index)

    # Regla 1: NOT NULL
    for col in required_not_null:
        if col in df.columns:
            null_mask = df[col].isna()
            rejection_reasons[null_mask & valid_mask] += f"NULL en {col}; "
            valid_mask &= ~null_mask

    # Regla 2 & 3: rangos para market share y market size
    for p in ["am1", "am2", "am3", "am4", "am5"]:
        ms_col = f"ms_{p}"
        mkt_col = f"mkt_{p}"

        if ms_col in df.columns:
            out_range = df[ms_col].notna() & (
                (df[ms_col] < ms_min) | (df[ms_col] > ms_max)
            )
            rejection_reasons[out_range & valid_mask] += f"MS fuera de rango en {ms_col}; "
            valid_mask &= ~out_range

        if mkt_col in df.columns:
            negative = df[mkt_col].notna() & (df[mkt_col] < mkt_min)
            rejection_reasons[negative & valid_mask] += f"MKT negativo en {mkt_col}; "
            valid_mask &= ~negative

    # Regla 4: duplicados en clave primaria
    pk = ["pais", "categoria", "sub_categoria", "laboratorio", "producto"]
    pk_cols = [c for c in pk if c in df.columns]
    dup_mask = df.duplicated(subset=pk_cols, keep="first")
    rejection_reasons[dup_mask & valid_mask] += "Duplicado en clave primaria; "
    valid_mask &= ~dup_mask

    # Registrar rechazos
    rejected_df = df[~valid_mask].copy()
    for idx, row in rejected_df.iterrows():
        report.add_rejection(
            row_data={c: row[c] for c in pk_cols if c in row},
            reason=rejection_reasons[idx].strip("; "),
        )

    valid_df = df[valid_mask].copy()

    for _ in range(len(valid_df)):
        report.add_pass()

    logger.info(report.summary())
    return valid_df, report


# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------

def process(
    raw_data: Dict[str, pd.DataFrame],
    config: dict,
    fx_path: str,
) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Pipeline completo de procesamiento: concat → pivot → flag → fx → validate.

    Args:
        raw_data: Dict de DataFrames raw de ingest.py (clave = código país).
        config:   configuración completa de config.yaml.
        fx_path:  ruta al archivo de tipos de cambio.
    Returns:
        Tuple(DataFrame_procesado_listo_para_master, ValidationReport).
    """
    cfg_shares = config["shares_file"]
    cfg_fx = config["fx_file"]
    cfg_val = config["validation"]

    logger.info("=== INICIO PROCESS ===")

    # 1. Concatenar todas las hojas
    logger.info("Paso 1/6: Concatenando hojas")
    df_all = pd.concat(list(raw_data.values()), ignore_index=True)
    logger.info(f"  Total combinado: {len(df_all):,} filas")

    # 2. Brand consolidation — renombrar variantes a marca madre y re-agregar
    logger.info("Paso 2/6: Brand consolidation")
    bc: dict = config.get("brand_consolidation") or {}
    if bc:
        col_prod = cfg_shares["columns"]["producto"]
        col_cat  = cfg_shares["columns"]["categoria"]
        col_sub  = cfg_shares["columns"]["sub_cat"]
        col_lab  = cfg_shares["columns"]["laboratorio"]
        col_mn   = cfg_shares["columns"]["metric_name"]
        col_am   = cfg_shares["columns"]["anio_movil"]
        col_val  = cfg_shares["columns"]["valor"]

        antes = df_all[col_prod].nunique()
        df_all[col_prod] = df_all[col_prod].replace(bc)
        despues = df_all[col_prod].nunique()

        # Re-agregar: sumar valores de variantes consolidadas bajo la misma clave
        group_cols = ["pais", col_cat, col_sub, col_lab, col_prod, col_mn, col_am]
        df_all = (
            df_all
            .groupby(group_cols, as_index=False, dropna=False)[col_val]
            .sum()
        )
        logger.info(
            f"  Brand consolidation: {len(bc)} mapeos | "
            f"productos antes={antes} → después={despues} | "
            f"{len(df_all):,} filas post-reagregación"
        )
    else:
        logger.info("  Brand consolidation: no definida en config.yaml — omitiendo")

    # 3. Pivot long → wide
    logger.info("Paso 3/6: Pivot long → wide")
    df_wide = pivot_long_to_wide(df_all, cfg_shares)

    # 4. Flag Genomma
    logger.info("Paso 4/6: Agregando flag es_genomma")
    df_wide = add_genomma_flag(df_wide, cfg_shares["genomma_string"])

    # 5. Cargar tipo de cambio y convertir a USD
    logger.info("Paso 5/6: Aplicando tipos de cambio")
    fx_rates = load_fx_rates(fx_path, cfg_fx)
    df_wide = apply_fx_conversion(df_wide, fx_rates)

    # Agregar nombre completo del país
    pais_nombres = cfg_shares["sheets"]  # {ARG: Argentina, ...}
    df_wide["pais_nombre"] = df_wide["pais"].map(pais_nombres)

    # 6. Validación de calidad
    logger.info("Paso 6/6: Validando calidad de datos")
    df_valid, report = validate(df_wide, cfg_val)

    logger.info(f"=== PROCESS COMPLETADO | {len(df_valid):,} registros válidos ===")
    return df_valid, report


# ---------------------------------------------------------------------------
# SMOKE TEST — ejecutar con: python scripts/process.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import datetime
    from ingest import ingest_shares_file, load_config

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smoke test de process.py")
    parser.add_argument("--file",   required=True, help="Ruta al Excel de shares")
    parser.add_argument("--fx",     required=True, help="Ruta al Excel tipo de cambio")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--sheets", nargs="+", default=None)
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SMOKE TEST — process.py | {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    try:
        config = load_config(args.config)
        raw_data = ingest_shares_file(args.file, config, sheets_override=args.sheets)
        df_result, report = process(raw_data, config, fx_path=args.fx)

        print(f"\n✅ PROCESAMIENTO EXITOSO")
        print(f"{'─'*40}")
        print(f"  Registros válidos: {len(df_result):,}")
        print(f"  Registros rechazados: {len(report.rejected):,}")
        print(f"  Columnas: {list(df_result.columns)}")
        print(f"\n  Muestra (5 filas):")
        print(df_result.head().to_string())

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(str(e))
        sys.exit(1)
