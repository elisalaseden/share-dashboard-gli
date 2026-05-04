"""
process_pcbev.py — Normalización, cálculo AM, MS% y tipo de cambio para PC/Bebidas
Share Dashboard · Investor Reporting Pipeline

Responsabilidad ÚNICA: recibir el DataFrame raw de ingest_pcbev.py,
calcular períodos AM1-AM5 dinámicamente, agregar por clave de negocio,
calcular market share, aplicar FX y retornar DataFrame listo para master_pcbev.csv.

NO lee Excel crudo. NO escribe a disco. NO construye visualizaciones.
Schema de salida: idéntico al master OTC para compatibilidad con app.py.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EXCEPCIONES PROPIAS
# ---------------------------------------------------------------------------

class PCBevProcessError(Exception):
    """Error genérico de procesamiento PC/Bev."""
    pass


class PCBevFXError(Exception):
    """Error cargando tipo de cambio para BRA."""
    pass


class ValidationReportPCBev:
    """Acumula resultados de validación sin interrumpir el pipeline."""

    def __init__(self) -> None:
        self.passed: int = 0
        self.rejected: List[dict] = []

    def add_rejection(self, row_data: dict, reason: str) -> None:
        self.rejected.append({"reason": reason, **row_data})
        logger.warning(f"Registro rechazado: {reason} | {row_data}")

    def add_pass(self) -> None:
        self.passed += 1

    def summary(self) -> str:
        total = self.passed + len(self.rejected)
        return (
            f"Validación PC/Bev: {self.passed:,}/{total:,} válidos | "
            f"{len(self.rejected):,} rechazados"
        )


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
        raise FileNotFoundError(f"config.yaml no encontrado: {path.absolute()}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# TIPO DE CAMBIO — BRA
# ---------------------------------------------------------------------------

def load_fx_bra(fx_path: str, cfg_fx: dict) -> float:
    """
    Extrae el tipo de cambio BRL/USD más reciente del archivo oficial Genomma.

    Args:
        fx_path: ruta al Excel de tipos de cambio.
        cfg_fx:  sección 'fx_file' del config (misma que usa el pipeline OTC).
    Returns:
        ER BRA como float.
    Raises:
        PCBevFXError: si el archivo no existe, la columna está vacía o el valor es inválido.
    """
    path = Path(fx_path)
    if not path.exists():
        raise PCBevFXError(f"Archivo tipo de cambio no encontrado: {path.absolute()}")

    logger.info(f"Cargando ER BRA desde: {fx_path}")

    try:
        df_raw = pd.read_excel(fx_path, sheet_name=cfg_fx["sheet"], header=None, engine="openpyxl")
    except Exception as e:
        raise PCBevFXError(f"Error leyendo hoja '{cfg_fx['sheet']}': {e}") from e

    data = df_raw.iloc[cfg_fx["header_row"]:, :].copy()
    date_col = cfg_fx["date_col"]
    data = data[data.iloc[:, date_col].notna()].reset_index(drop=True)

    if data.empty:
        raise PCBevFXError("No se encontraron filas con fecha válida en el archivo de tipo de cambio.")

    last_row = data.iloc[-1]
    fecha = last_row.iloc[date_col]

    bra_col = cfg_fx["er_columns"]["BRA"]
    er_val = last_row.iloc[bra_col]

    if pd.isna(er_val) or float(er_val) <= 0:
        raise PCBevFXError(
            f"ER BRA inválido: valor={er_val} en col={bra_col}. "
            f"Verifica el archivo de tipo de cambio."
        )

    er = float(er_val)
    logger.info(f"ER BRA aplicado: {er:.4f} BRL/USD | período: {fecha}")
    return er


# ---------------------------------------------------------------------------
# DETECCIÓN DINÁMICA DE PERÍODOS AM
# ---------------------------------------------------------------------------

def compute_am_ranges(
    df: pd.DataFrame,
    periodo_col: str,
    am_window_months: int,
    am_periods: int,
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Calcula los rangos de fecha para AM1..AMN desde el período más reciente
    del dataset.

    AM1 = [max_period - (window-1) meses, max_period]
    AM2 = [max_period - (2*window-1) meses, max_period - window meses]
    ... etc.

    Args:
        df:                DataFrame con columna de período en formato MM/YYYY.
        periodo_col:       nombre de la columna de período.
        am_window_months:  meses por ventana (config: am_window_months = 12).
        am_periods:        número de períodos AM (config: am_periods = 5).
    Returns:
        Lista de tuplas (fecha_inicio, fecha_fin) para AM1..AMN.
        AM1 = índice 0, AM5 = índice 4.
    Raises:
        PCBevProcessError: si no hay fechas válidas en la columna de período.
    """
    fechas = pd.to_datetime(df[periodo_col], format="%m/%Y", errors="coerce")
    fechas = fechas.dropna()

    if fechas.empty:
        raise PCBevProcessError(f"No hay fechas válidas en la columna '{periodo_col}'.")

    max_fecha = fechas.max()
    logger.info(f"Período más reciente detectado: {max_fecha.strftime('%m/%Y')}")

    ranges = []
    for i in range(am_periods):
        fin = max_fecha - pd.DateOffset(months=i * am_window_months)
        inicio = fin - pd.DateOffset(months=am_window_months - 1)
        # Normalizar al primer día del mes
        fin = fin.replace(day=1)
        inicio = inicio.replace(day=1)
        ranges.append((inicio, fin))
        logger.info(
            f"  AM{i+1}: {inicio.strftime('%m/%Y')} → {fin.strftime('%m/%Y')}"
        )

    return ranges


# ---------------------------------------------------------------------------
# ASIGNACIÓN DE AM A CADA FILA
# ---------------------------------------------------------------------------

def assign_am_labels(
    df: pd.DataFrame,
    periodo_col: str,
    am_ranges: List[Tuple[pd.Timestamp, pd.Timestamp]],
) -> pd.DataFrame:
    """
    Agrega columna 'am_label' (am1..am5 o NaN) a cada fila según
    en qué ventana cae su período.

    Args:
        df:          DataFrame con columna de período.
        periodo_col: nombre de la columna de período.
        am_ranges:   lista de tuplas (inicio, fin) para AM1..AMN.
    Returns:
        DataFrame con columna 'am_label'. Filas fuera de rango quedan NaN.
    """
    df = df.copy()
    df["_fecha"] = pd.to_datetime(df[periodo_col], format="%m/%Y", errors="coerce")
    df["am_label"] = pd.NA

    for i, (inicio, fin) in enumerate(am_ranges):
        mask = (df["_fecha"] >= inicio) & (df["_fecha"] <= fin)
        df.loc[mask, "am_label"] = f"am{i+1}"

    fuera_rango = df["am_label"].isna().sum()
    if fuera_rango > 0:
        logger.warning(
            f"{fuera_rango:,} filas fuera de rango AM (anteriores a AM{len(am_ranges)}) — excluidas del master."
        )

    df = df.drop(columns=["_fecha"])
    return df


# ---------------------------------------------------------------------------
# CORRECCIONES DE PRODUCTO — aplicar antes del market_latam_map
# ---------------------------------------------------------------------------

def apply_product_corrections(df: pd.DataFrame, corrections: dict, cols: dict) -> pd.DataFrame:
    """
    Aplica correcciones de producto en orden:
        1. exclude        — elimina filas con dato incorrecto
        2. reclassify     — corrige market de filas mal clasificadas
        3. produto_rename — consolida marcas bajo nombre canónico

    Args:
        df:          DataFrame raw post-ingest.
        corrections: sección 'product_corrections' del config.
        cols:        sección 'columns' del config (nombres de columnas canónicas).
    Returns:
        DataFrame corregido listo para market_latam_map.
    """
    market_col  = cols["market"]
    produto_col = cols["produto"]
    df = df.copy()
    n_inicial = len(df)

    # 1. Excluir filas con dato incorrecto (opcional — si la clave existe)
    for rule in corrections.get("exclude", []):
        mask = (df[produto_col] == rule["produto"]) & (df[market_col] == rule["market"])
        n_excluidas = mask.sum()
        if n_excluidas > 0:
            logger.info(
                f"  [EXCLUDE] {rule['produto']} | {rule['market']}: "
                f"{n_excluidas:,} filas eliminadas"
            )
        df = df[~mask].copy()

    # 2. Reclasificar market
    for rule in corrections.get("reclassify_market", []):
        mask = (df[produto_col] == rule["produto"]) & (df[market_col] == rule["from_market"])
        n_reclas = mask.sum()
        if n_reclas > 0:
            df.loc[mask, market_col] = rule["to_market"]
            logger.info(
                f"  [RECLASSIFY] {rule['produto']}: "
                f"{rule['from_market']} → {rule['to_market']} | {n_reclas:,} filas"
            )

    # 3. Renombrar produto
    for old_name, new_name in corrections.get("produto_rename", {}).items():
        mask = df[produto_col] == old_name
        n_rename = mask.sum()
        if n_rename > 0:
            df.loc[mask, produto_col] = new_name
            logger.info(
                f"  [RENAME] {old_name} → {new_name}: {n_rename:,} filas"
            )

    n_final = len(df)
    logger.info(
        f"Correcciones aplicadas: {n_inicial:,} → {n_final:,} filas "
        f"({n_inicial - n_final:,} eliminadas)"
    )
    return df


# ---------------------------------------------------------------------------
# AGREGACIÓN Y CÁLCULO DE MARKET SHARE
# ---------------------------------------------------------------------------

def aggregate_and_compute_ms(
    df: pd.DataFrame,
    cfg_pcbev: dict,
) -> pd.DataFrame:
    """
    Agrega el valor en moneda local por (pais, categoria, sub_categoria, laboratorio, produto, am_label)
    y calcula el market share % usando el total del market por categoria+am como denominador.

    Market Share % = SUM(valor_col for this product+am) / SUM(valor_col for all products same categoria+am) × 100

    Args:
        df:         DataFrame con columna am_label asignada.
        cfg_pcbev:  sección 'pcbev_file' del config.
    Returns:
        DataFrame wide con columnas mkt_am1..5 y ms_am1..5.
    """
    cols = cfg_pcbev["columns"]
    valor_col  = cfg_pcbev["valor_col"]
    latam_map  = cfg_pcbev["market_latam_map"]
    genomma_str = cfg_pcbev["genomma_string"]

    # Aplicar mapping market → categoria LATAM
    df = df.copy()
    df["categoria"] = df[cols["market"]].map(latam_map)
    unmapped = df[df["categoria"].isna()][cols["market"]].unique()
    if len(unmapped) > 0:
        logger.warning(f"Markets no mapeados a LATAM (serán excluidos): {list(unmapped)}")
        df = df[df["categoria"].notna()].copy()

    # Columnas de dimensión internas
    # sub_categoria NO aplica para PC/Bev (proveedores sin submarket estandarizado)
    df["laboratorio"] = df[cols["fabricante"]]
    df["produto"]     = df[cols["produto"]]

    # Filtrar solo filas con am_label válido
    df = df[df["am_label"].notna()].copy()
    if df.empty:
        raise PCBevProcessError("Sin filas con am_label válido tras asignación de períodos.")

    id_vars = ["pais", "categoria", "laboratorio", "produto"]
    group_cols = id_vars + ["am_label"]

    # Paso 1: agregar valor por producto × AM
    product_agg = (
        df.groupby(group_cols)[valor_col]
        .sum()
        .reset_index()
        .rename(columns={valor_col: "mkt_value"})
    )

    # Paso 2: total del market por (pais, categoria, am_label) → denominador del MS%
    market_total = (
        df.groupby(["pais", "categoria", "am_label"])[valor_col]
        .sum()
        .reset_index()
        .rename(columns={valor_col: "cat_total"})
    )

    # Paso 3: unir y calcular MS%
    merged = product_agg.merge(market_total, on=["pais", "categoria", "am_label"], how="left")

    # MS% = (valor producto / total categoría) × 100
    # Si cat_total es 0 o negativo: MS = NaN (no calculable)
    merged["ms_value"] = np.where(
        merged["cat_total"].notna() & (merged["cat_total"] != 0),
        merged["mkt_value"] / merged["cat_total"] * 100,
        np.nan,
    )

    # Paso 4: pivot a wide — mkt_am1..5 y ms_am1..5
    mkt_wide = merged.pivot_table(
        index=id_vars,
        columns="am_label",
        values="mkt_value",
        aggfunc="first",
    ).reset_index()
    mkt_wide.columns.name = None

    ms_wide = merged.pivot_table(
        index=id_vars,
        columns="am_label",
        values="ms_value",
        aggfunc="first",
    ).reset_index()
    ms_wide.columns.name = None

    # Renombrar columnas: am1 → mkt_am1 y ms_am1
    am_cols = [f"am{i}" for i in range(1, cfg_pcbev["am_periods"] + 1)]

    for col in am_cols:
        if col in mkt_wide.columns:
            mkt_wide = mkt_wide.rename(columns={col: f"mkt_{col}"})
        else:
            mkt_wide[f"mkt_{col}"] = np.nan

        if col in ms_wide.columns:
            ms_wide = ms_wide.rename(columns={col: f"ms_{col}"})
        else:
            ms_wide[f"ms_{col}"] = np.nan

    # Unir mkt + ms wide
    ms_cols_only = [c for c in ms_wide.columns if c.startswith("ms_")]
    df_wide = mkt_wide.merge(ms_wide[id_vars + ms_cols_only], on=id_vars, how="left")

    # Renombrar 'produto' → 'producto' para compatibilidad con schema master OTC
    df_wide = df_wide.rename(columns={"produto": "producto"})

    logger.info(
        f"Agregación completada: {len(df_wide):,} combinaciones únicas "
        f"(pais × categoria × laboratorio × producto)"
    )
    return df_wide


# ---------------------------------------------------------------------------
# FLAG GENOMMA
# ---------------------------------------------------------------------------

def add_genomma_flag(df: pd.DataFrame, genomma_string: str) -> pd.DataFrame:
    """
    Agrega columna boolean es_genomma.

    Args:
        df:             DataFrame wide.
        genomma_string: string exacto desde config.
    Returns:
        DataFrame con columna 'es_genomma' agregada.
    """
    df = df.copy()
    df["es_genomma"] = df["laboratorio"].str.upper() == genomma_string.upper()
    n_gli = df["es_genomma"].sum()
    logger.info(f"Flag Genomma: {n_gli:,} productos identificados como GLI")
    return df


# ---------------------------------------------------------------------------
# CONVERSIÓN A USD
# ---------------------------------------------------------------------------

def apply_fx_conversion(df: pd.DataFrame, er_bra: float, am_periods: int) -> pd.DataFrame:
    """
    Convierte columnas mkt_amN a USD usando el ER oficial Genomma para BRA.

    Args:
        df:         DataFrame wide post-agregación.
        er_bra:     tipo de cambio BRL/USD.
        am_periods: número de períodos AM.
    Returns:
        DataFrame con columnas mkt_usd_amN y er_aplicado.
    """
    df = df.copy()
    df["er_aplicado"] = er_bra

    for i in range(1, am_periods + 1):
        mkt_col = f"mkt_am{i}"
        usd_col = f"mkt_usd_am{i}"
        if mkt_col in df.columns:
            df[usd_col] = df[mkt_col] / er_bra
        else:
            df[usd_col] = np.nan

    logger.info(f"Conversión USD completada | ER BRA: {er_bra:.4f}")
    return df


# ---------------------------------------------------------------------------
# VALIDACIÓN
# ---------------------------------------------------------------------------

def validate_pcbev(
    df: pd.DataFrame,
    cfg_val: dict,
) -> Tuple[pd.DataFrame, ValidationReportPCBev]:
    """
    Valida el DataFrame procesado aplicando reglas de validation_pcbev del config.

    Reglas activas para PC/Bev:
        1. Completitud: columnas required_not_null no tienen nulos.
        2. Duplicados:  clave primaria única.
    Reglas NO aplicadas (diferencia vs OTC):
        - mkt_min: negativos son datos válidos (D6 confirmado).
        - ms_range: MS calculado internamente puede ser negativo si cat_total < 0.

    Args:
        df:      DataFrame wide post-conversión.
        cfg_val: sección 'validation_pcbev' del config.
    Returns:
        Tuple(DataFrame_válido, ValidationReportPCBev).
    """
    report = ValidationReportPCBev()
    required = cfg_val.get("required_not_null", [])
    pk = ["pais", "categoria", "laboratorio", "producto"]

    valid_mask = pd.Series(True, index=df.index)
    rejection_reasons = pd.Series("", index=df.index)

    # Regla 1: NOT NULL — con excepción para GLI
    # mkt_am1 es obligatorio para competencia pero NO para GLI:
    # toda marca Genomma se reporta aunque sea residual o discontinuada.
    for col in required:
        actual_col = "producto" if col == "produto" else col
        if actual_col not in df.columns:
            continue

        null_mask = df[actual_col].isna()

        if actual_col == "mkt_am1" and "es_genomma" in df.columns:
            # Aplicar la regla solo a non-GLI
            null_mask = null_mask & ~df["es_genomma"]

        rejection_reasons[null_mask & valid_mask] += f"NULL en {actual_col}; "
        valid_mask &= ~null_mask

    # Regla 2: duplicados en PK
    pk_present = [c for c in pk if c in df.columns]
    dup_mask = df.duplicated(subset=pk_present, keep="first")
    rejection_reasons[dup_mask & valid_mask] += "Duplicado en clave primaria; "
    valid_mask &= ~dup_mask

    rejected_df = df[~valid_mask].copy()
    for idx, row in rejected_df.iterrows():
        report.add_rejection(
            row_data={c: row[c] for c in pk_present if c in row},
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

def process_pcbev(
    df_raw: pd.DataFrame,
    config: dict,
    fx_path: str,
) -> Tuple[pd.DataFrame, ValidationReportPCBev]:
    """
    Pipeline completo de procesamiento PC/Bev:
    AM detection → assign → aggregate + MS% → flag → FX → validate.

    Args:
        df_raw:   DataFrame consolidado de ingest_pcbev.py.
        config:   configuración completa de config.yaml.
        fx_path:  ruta al archivo de tipos de cambio.
    Returns:
        Tuple(DataFrame_procesado_listo_para_master_pcbev, ValidationReportPCBev).
    """
    cfg_pcbev = config["pcbev_file"]
    cfg_fx    = config["fx_file"]
    cfg_val   = config["validation_pcbev"]

    periodo_col      = cfg_pcbev["columns"]["periodo"]
    am_window_months = cfg_pcbev["am_window_months"]
    am_periods       = cfg_pcbev["am_periods"]

    logger.info("=== INICIO PROCESS PC/BEV ===")
    logger.info(f"  Filas raw recibidas: {len(df_raw):,}")

    # Paso 1: Detectar rangos AM dinámicamente
    logger.info("Paso 1/7: Calculando rangos de períodos AM")
    am_ranges = compute_am_ranges(df_raw, periodo_col, am_window_months, am_periods)

    # Paso 2: Asignar am_label a cada fila
    logger.info("Paso 2/7: Asignando am_label por período")
    df_labeled = assign_am_labels(df_raw, periodo_col, am_ranges)

    # Paso 3: Correcciones de producto (exclude / reclassify / rename)
    logger.info("Paso 3/7: Aplicando correcciones de producto")
    corrections = cfg_pcbev.get("product_corrections", {})
    df_labeled = apply_product_corrections(df_labeled, corrections, cfg_pcbev["columns"])

    # Paso 4: Agregar + calcular MS%
    logger.info("Paso 4/7: Agregando y calculando market share")
    df_wide = aggregate_and_compute_ms(df_labeled, cfg_pcbev)

    # Paso 5: Flag Genomma
    logger.info("Paso 5/7: Agregando flag es_genomma")
    df_wide = add_genomma_flag(df_wide, cfg_pcbev["genomma_string"])

    # Paso 6: FX → USD
    logger.info("Paso 6/7: Aplicando tipo de cambio BRA")
    er_bra = load_fx_bra(fx_path, cfg_fx)
    df_wide = apply_fx_conversion(df_wide, er_bra, am_periods)

    # Agregar nombre del país
    df_wide["pais_nombre"] = cfg_pcbev["pais_nombre"]

    # Paso 7: Validación
    logger.info("Paso 7/7: Validando calidad")
    df_valid, report = validate_pcbev(df_wide, cfg_val)

    logger.info(
        f"=== PROCESS PC/BEV COMPLETADO | {len(df_valid):,} registros válidos ==="
    )
    return df_valid, report


# ---------------------------------------------------------------------------
# SMOKE TEST — ejecutar con: python scripts/process_pcbev.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    sys.path.insert(0, str(Path(__file__).parent))
    from ingest_pcbev import ingest_pcbev

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smoke test de process_pcbev.py")
    parser.add_argument("--files",  nargs="+", required=True, help="Archivos Excel PC/Bev")
    parser.add_argument("--fx",     required=True, help="Excel tipo de cambio")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"SMOKE TEST — process_pcbev.py | {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    try:
        config = load_config(args.config)
        df_raw  = ingest_pcbev(args.files, config)
        df_result, report = process_pcbev(df_raw, config, fx_path=args.fx)

        cfg = config["pcbev_file"]
        am_periods = cfg["am_periods"]

        print(f"\n✅ PROCESAMIENTO EXITOSO")
        print(f"{'─'*40}")
        print(f"  Registros válidos:    {len(df_result):,}")
        print(f"  Registros rechazados: {len(report.rejected):,}")
        print(f"  ER BRA aplicado:      {df_result['er_aplicado'].iloc[0]:.4f}")
        print(f"  Categorías LATAM:     {sorted(df_result['categoria'].unique())}")
        print(f"  Sub-categorías:       {sorted(df_result['sub_categoria'].unique())}")
        print(f"  Productos GLI:        {df_result['es_genomma'].sum():,}")
        print(f"\n  Columnas: {list(df_result.columns)}")
        print(f"\n  Muestra GLI (5 filas):")
        gli = df_result[df_result['es_genomma']].head(5)
        print(gli[["categoria","sub_categoria","laboratorio","producto",
                    "mkt_am1","ms_am1","mkt_usd_am1"]].to_string())

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(str(e))
        sys.exit(1)
