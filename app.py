"""
app.py — Dashboard Streamlit · Share Dashboard OTC LATAM
Share Dashboard · Investor Reporting Pipeline — v2.0

Reescrito desde cero. NO parchear — reemplazar completo.

Hojas:
  1. Tamaño de Categoría  — mercado total jerárquico (cat → sub-cat)
  2. GLI — Ventas Genomma — ventas GLI jerárquicas (cat → sub-cat)
  3. Shares GLI por Marca — shares 3 niveles (cat → sub-cat → marca)

Reglas de negocio:
  - Share GLI = sum(mkt GLI) / sum(mkt total). NUNCA sum(ms_amN).
  - am1 = MAT más reciente (reporting_year). am5 = MAT más antiguo.
  - Columnas ordenadas cronológicamente: am5 → am4 → am3 → am2 → am1.
  - Datos NO se ajustan, no se snapean, no se excluyen.

Ejecutar:
    streamlit run app.py  (desde raíz del proyecto)
"""

import io
import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import yaml

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────────────────
MASTER_PATH = Path("data/master.csv")
CONFIG_PATH = Path("config.yaml")

# ─────────────────────────────────────────────────────────────────────────────
# COLORES (del mockup confirmado — no modificar sin aprobación)
# ─────────────────────────────────────────────────────────────────────────────
C_CAT_BG  = "#D4AC0D"   # categoría madre — amarillo dorado
C_CAT_FG  = "#ffffff"
C_SUB_BG  = "#D6EAF8"   # sub-categoría — azul claro
C_SUB_FG  = "#1B4F72"
C_MRC_BG  = "#ffffff"   # marca — blanco
C_MRC_FG  = "#333333"
C_NON_BG  = "#f8f8f8"   # sin presencia GLI — gris
C_NON_FG  = "#999999"
C_TOT_BG  = "#D5F5E3"   # total — verde claro
C_TOT_FG  = "#1E8449"
C_GLI_BG  = "#1B4F72"   # categoría en hoja GLI — azul oscuro
C_GLI_FG  = "#ffffff"
C_GREEN   = "#1E8449"
C_RED     = "#C0392B"
C_GRAY    = "#999999"

# CSS compartido para todas las tablas — se inyecta una sola vez en main()
_TABLE_CSS = """
<style>
.shr-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11px;
    font-family: Arial, sans-serif;
    margin-bottom: 8px;
}
.shr-table th {
    background: #2C3E50;
    color: #fff;
    padding: 6px 8px;
    text-align: right;
    white-space: nowrap;
    font-size: 10px;
    letter-spacing: 0.03em;
}
.shr-table th:first-child,
.shr-table th:nth-child(2) { text-align: left; }
.shr-table td {
    padding: 5px 8px;
    border-bottom: 1px solid #eee;
    text-align: right;
    white-space: nowrap;
}
.shr-table td:first-child,
.shr-table td:nth-child(2) { text-align: left; }
</style>
"""

PAISES_LABELS = {
    "ARG": "🇦🇷 Argentina",
    "BRA": "🇧🇷 Brasil",
    "CHI": "🇨🇱 Chile",
    "COL": "🇨🇴 Colombia",
    "ECU": "🇪🇨 Ecuador",
    "MEX": "🇲🇽 México",
    "PER": "🇵🇪 Perú",
}


# ─────────────────────────────────────────────────────────────────────────────
# CARGA — CONFIG Y MASTER
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data
def load_config() -> dict:
    """Carga config.yaml desde disco."""
    if not CONFIG_PATH.exists():
        st.error(f"config.yaml no encontrado: {CONFIG_PATH.absolute()}")
        st.stop()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_data(ttl=3600)
def load_master() -> pd.DataFrame:
    """
    Carga master.csv desde disco con caché de 1 hora.

    Garantiza:
      - es_genomma como bool nativo.
      - Columnas numéricas como float.
    """
    if not MASTER_PATH.exists():
        st.error(
            f"master.csv no encontrado en {MASTER_PATH.absolute()}. "
            "Ejecuta el pipeline primero."
        )
        st.stop()
    df = pd.read_csv(MASTER_PATH, low_memory=False)
    # Normalizar es_genomma — puede llegar como string "True"/"False" desde CSV
    df["es_genomma"] = df["es_genomma"].astype(str).str.strip().str.lower() == "true"
    return df


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE CÁLCULO
# ─────────────────────────────────────────────────────────────────────────────

def cagr(v_final: float, v_ini: float, n: int) -> float:
    """
    CAGR estándar.

    Returns:
        float ratio (no porcentaje). NaN si inputs inválidos.
    """
    if pd.isna(v_final) or pd.isna(v_ini) or v_ini <= 0 or n <= 0:
        return np.nan
    return (v_final / v_ini) ** (1 / n) - 1


def delta_yoy(v_new: float, v_old: float) -> float:
    """
    Crecimiento YoY como ratio.

    Returns:
        float ratio. NaN si v_old == 0 o cualquier input inválido.
    """
    if pd.isna(v_new) or pd.isna(v_old) or v_old == 0:
        return np.nan
    return (v_new / v_old) - 1


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE FORMATO
# ─────────────────────────────────────────────────────────────────────────────

def fmt_mill(v: float) -> str:
    """Formatea valor numérico grande en K / M / B / T. '—' si NaN."""
    if pd.isna(v):
        return "—"
    abs_v = abs(v)
    if abs_v >= 1e12:
        return f"{v / 1e12:.1f}T"
    if abs_v >= 1e9:
        return f"{v / 1e9:.1f}B"
    if abs_v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if abs_v >= 1e3:
        return f"{v / 1e3:.1f}K"
    return f"{v:,.0f}"


def fmt_pct(v: float) -> str:
    """Ratio → '+5.1%'. '—' si NaN."""
    if pd.isna(v):
        return "—"
    s = "+" if v >= 0 else ""
    return f"{s}{v * 100:.1f}%"


def fmt_shr(v: float) -> str:
    """Share como '3.45%'. '—' si NaN."""
    if pd.isna(v):
        return "—"
    return f"{v:.2f}%"


def fmt_pp(v: float) -> str:
    """Δ en puntos de share como '+1.23 pp'. '—' si NaN."""
    if pd.isna(v):
        return "—"
    s = "+" if v >= 0 else ""
    return f"{s}{v:.2f} pp"


def _span_color(text: str, value: float, positive_green: bool = True) -> str:
    """Envuelve texto en <span> con color verde/rojo según signo del valor."""
    if pd.isna(value):
        return f'<span style="color:{C_GRAY}">{text}</span>'
    if positive_green:
        color = C_GREEN if value >= 0 else C_RED
    else:
        color = C_RED if value >= 0 else C_GREEN
    return f'<span style="color:{color};font-weight:600">{text}</span>'


def colored_pct(v: float) -> str:
    """YoY formateado como <span> HTML con color."""
    return _span_color(fmt_pct(v), v)


def colored_pp(v: float) -> str:
    """Δ pp formateado como <span> HTML con color."""
    return _span_color(fmt_pp(v), v)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — FILTROS
# ─────────────────────────────────────────────────────────────────────────────

def sidebar_filters(df: pd.DataFrame) -> tuple:
    """
    Renderiza filtros en el sidebar.

    Lógica Toggle Total LATAM:
      ON  → multiselect países OCULTO (no disabled — oculto).
      OFF → multiselect países visible.

    Returns:
        (total_latam, paises_sel, cat_sel, subcat_sel, use_usd)
    """
    with st.sidebar:
        st.header("⚙️ Filtros")

        total_latam: bool = st.toggle("🌎 Total LATAM", value=True)

        paises_disp = sorted(df["pais"].unique())
        paises_sel: list = paises_disp  # default: todos los países

        if not total_latam:
            paises_sel = st.multiselect(
                "País",
                options=paises_disp,
                format_func=lambda x: PAISES_LABELS.get(x, x),
                default=paises_disp,
            )

        cats_disp = sorted(df["categoria"].unique())
        cat_sel: list = st.multiselect(
            "Categoría Madre",
            options=cats_disp,
            default=cats_disp,
        )

        # Sub-categoría dinámica según categorías seleccionadas
        df_cat = df[df["categoria"].isin(cat_sel)] if cat_sel else df
        subcats_disp = sorted(df_cat["sub_categoria"].unique())
        subcat_sel: list = st.multiselect(
            "Sub-categoría",
            options=subcats_disp,
            default=subcats_disp,
        )

        st.divider()

        use_usd: bool = st.toggle("💵 Mostrar en USD", value=False)
        st.caption(
            "**USD:** tipo de cambio oficial Genomma (ER Mar-2026).\n\n"
            "**Local:** moneda original del archivo CloseUp."
        )

        st.divider()
        st.caption(
            f"**Dataset cargado:**\n"
            f"- {len(df):,} registros\n"
            f"- {df['pais'].nunique()} países\n"
            f"- {df['categoria'].nunique()} categorías\n"
            f"- {df['producto'].nunique():,} productos"
        )

    return total_latam, paises_sel, cat_sel, subcat_sel, use_usd


def apply_filters(
    df: pd.DataFrame,
    total_latam: bool,
    paises_sel: list,
    cat_sel: list,
    subcat_sel: list,
) -> pd.DataFrame:
    """
    Aplica filtros al DataFrame. Función pura — no modifica el original.

    Args:
        df:          DataFrame master completo.
        total_latam: si True, no filtrar por país.
        paises_sel:  lista de códigos de país.
        cat_sel:     lista de categorías madres.
        subcat_sel:  lista de sub-categorías.
    Returns:
        DataFrame filtrado.
    """
    df_f = df.copy()
    if not total_latam and paises_sel:
        df_f = df_f[df_f["pais"].isin(paises_sel)]
    if cat_sel:
        df_f = df_f[df_f["categoria"].isin(cat_sel)]
    if subcat_sel:
        df_f = df_f[df_f["sub_categoria"].isin(subcat_sel)]
    return df_f


def get_pais_label(total_latam: bool, paises_sel: list) -> str:
    """Label de país para subtítulo y columna País en tablas."""
    if total_latam:
        return "Total LATAM"
    if len(paises_sel) == 1:
        return PAISES_LABELS.get(paises_sel[0], paises_sel[0])
    if len(paises_sel) == 0:
        return "Sin selección"
    return f"{len(paises_sel)} países"


# ─────────────────────────────────────────────────────────────────────────────
# HEADER — 4 KPIs
# ─────────────────────────────────────────────────────────────────────────────

def compute_header_kpis(df: pd.DataFrame, prefix: str) -> dict:
    """
    Calcula los 4 KPIs del header.

    REGLA: Share GLI = sum(mkt_GLI) / sum(mkt_total). NUNCA sum(ms_amN).

    Args:
        df:     DataFrame filtrado.
        prefix: 'mkt' o 'mkt_usd'.
    Returns:
        Dict con métricas del header.
    """
    c1 = f"{prefix}_am1"
    c2 = f"{prefix}_am2"
    c5 = f"{prefix}_am5"

    total_am1 = df[c1].sum(min_count=1)
    total_am2 = df[c2].sum(min_count=1)
    total_am5 = df[c5].sum(min_count=1)

    gli = df[df["es_genomma"]]
    gli_am1 = gli[c1].sum(min_count=1)
    gli_am2 = gli[c2].sum(min_count=1)
    gli_am5 = gli[c5].sum(min_count=1)

    share_am1 = (gli_am1 / total_am1 * 100) if (pd.notna(total_am1) and total_am1 > 0) else np.nan
    share_am2 = (gli_am2 / total_am2 * 100) if (pd.notna(total_am2) and total_am2 > 0) else np.nan
    delta_shr = (share_am1 - share_am2) if (pd.notna(share_am1) and pd.notna(share_am2)) else np.nan

    return {
        "total_am1":   total_am1,
        "total_yoy":   delta_yoy(total_am1, total_am2),
        "gli_am1":     gli_am1,
        "gli_yoy":     delta_yoy(gli_am1, gli_am2),
        "share_am1":   share_am1,
        "delta_share": delta_shr,
        "cagr_mkt_5y": cagr(total_am1, total_am5, 5),
        "cagr_gli_5y": cagr(gli_am1, gli_am5, 5),
    }


def render_header(
    kpis: dict,
    use_usd: bool,
    pais_label: str,
    reporting_year: int,
) -> None:
    """Renderiza el header con título y 4 KPI boxes."""

    def _delta(v: float, fmt_fn, invert: bool = False) -> str:
        if pd.isna(v):
            return f'<span style="color:{C_GRAY};font-size:10px">—</span>'
        color = C_GREEN if (v >= 0) != invert else C_RED
        return f'<span style="color:{color};font-size:10px">{fmt_fn(v)}</span>'

    usd_note = " (USD)" if use_usd else ""
    yr = reporting_year

    share_str  = fmt_shr(kpis["share_am1"])
    cagr_mkt   = fmt_pct(kpis["cagr_mkt_5y"])
    cagr_gli   = fmt_pct(kpis["cagr_gli_5y"])

    html = f"""
    <div style="background:#fff;border:1px solid #ddd;border-radius:8px;overflow:hidden;margin-bottom:16px">
      <div style="padding:14px 20px;border-bottom:1px solid #eee">
        <div style="font-size:20px;font-weight:700;color:#1a1a1a;margin-bottom:2px">
          📊 Share Dashboard — OTC LATAM · Investor Reporting
        </div>
        <div style="font-size:11px;color:#666">
          Genomma Lab Internacional · Consumer Insights · {pais_label} · MAT {yr}{usd_note}
        </div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr)">
        <div style="padding:12px 16px;border-right:1px solid #ddd">
          <div style="font-size:10px;color:#666;margin-bottom:4px">Mercado Total MAT {yr}</div>
          <div style="font-size:22px;font-weight:700;color:#1a1a1a">{fmt_mill(kpis['total_am1'])}</div>
          <div style="margin-top:2px">{_delta(kpis['total_yoy'], fmt_pct)} YoY</div>
        </div>
        <div style="padding:12px 16px;border-right:1px solid #ddd">
          <div style="font-size:10px;color:#666;margin-bottom:4px">Venta GLI MAT {yr}</div>
          <div style="font-size:22px;font-weight:700;color:#1a1a1a">{fmt_mill(kpis['gli_am1'])}</div>
          <div style="margin-top:2px">{_delta(kpis['gli_yoy'], fmt_pct)} YoY</div>
        </div>
        <div style="padding:12px 16px;border-right:1px solid #ddd">
          <div style="font-size:10px;color:#666;margin-bottom:4px">Share GLI Total</div>
          <div style="font-size:22px;font-weight:700;color:#1a1a1a">{share_str}</div>
          <div style="margin-top:2px">{_delta(kpis['delta_share'], fmt_pp)} YoY</div>
        </div>
        <div style="padding:12px 16px">
          <div style="font-size:10px;color:#666;margin-bottom:4px">CAGR Mercado 5Y</div>
          <div style="font-size:22px;font-weight:700;color:#1a1a1a">{cagr_mkt}</div>
          <div style="font-size:10px;color:#666;margin-top:2px">GLI 5Y: {cagr_gli}</div>
        </div>
      </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HOJA 1 — TAMAÑO DE CATEGORÍA
# ─────────────────────────────────────────────────────────────────────────────

def _row_cat_html(
    label: str,
    vals_ordered: list,   # [v_am5, v_am4, v_am3, v_am2, v_am1]
    pais_label: str,
    row_type: str,        # "cat" | "sub" | "total" | "mrc"
) -> str:
    """
    Construye una fila <tr> para las tablas de Categoría y GLI.
    vals_ordered debe estar en orden cronológico: am5 → am1.
    """
    v5, v4, v3, v2, v1 = vals_ordered
    yoy = delta_yoy(v1, v2)
    c3y = cagr(v1, v3, 3)
    c5y = cagr(v1, v5, 5)

    if row_type == "cat":
        bg, fg, fw, label_str = C_CAT_BG, C_CAT_FG, "700", label
    elif row_type == "cat_gli":
        bg, fg, fw, label_str = C_GLI_BG, C_GLI_FG, "700", label
    elif row_type == "sub":
        bg, fg, fw, label_str = C_SUB_BG, C_SUB_FG, "600", f"▸ {label}"
    elif row_type == "total":
        bg, fg, fw, label_str = C_TOT_BG, C_TOT_FG, "700", label
    else:  # mrc / default
        bg, fg, fw, label_str = C_MRC_BG, C_MRC_FG, "400", label

    yoy_html = colored_pct(yoy)

    return (
        f'<tr style="background:{bg};color:{fg};font-weight:{fw}">'
        f"<td>{label_str}</td>"
        f"<td>{pais_label}</td>"
        f"<td>{fmt_mill(v5)}</td>"
        f"<td>{fmt_mill(v4)}</td>"
        f"<td>{fmt_mill(v3)}</td>"
        f"<td>{fmt_mill(v2)}</td>"
        f"<td>{fmt_mill(v1)}</td>"
        f"<td>{yoy_html}</td>"
        f"<td>{fmt_pct(c3y)}</td>"
        f"<td>{fmt_pct(c5y)}</td>"
        f"</tr>"
    )


def _no_gli_row_html(label: str, indent: int = 1) -> str:
    """Fila gris 'Sin presencia GLI' para tablas de GLI y Shares."""
    pad = "▸ " if indent == 1 else ("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" if indent == 2 else "")
    return (
        f'<tr style="background:{C_NON_BG};color:{C_NON_FG};font-style:italic">'
        f"<td>{pad}{label}</td>"
        f'<td colspan="9" style="text-align:left">Sin presencia GLI</td>'
        f"</tr>"
    )


def build_category_table(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> str:
    """
    Tabla HTML Hoja 1 — Tamaño de Categoría.
    Jerarquía: Categoría (amarillo) → Sub-categoría (azul) → Total (verde).

    Args:
        df:             DataFrame filtrado.
        prefix:         'mkt' o 'mkt_usd'.
        reporting_year: año correspondiente a am1.
        pais_label:     etiqueta de país/selección para columna País.
    Returns:
        HTML string completo.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]  # am5→am1 (cronológico)
    yr = reporting_year

    cat_agg = (
        df.groupby("categoria")[periods]
        .sum()
        .reset_index()
        .sort_values("categoria")
    )
    sub_agg = (
        df.groupby(["categoria", "sub_categoria"])[periods]
        .sum()
        .reset_index()
        .sort_values(["categoria", "sub_categoria"])
    )

    rows_html: list[str] = []

    for _, cat_row in cat_agg.iterrows():
        cat = cat_row["categoria"]
        rows_html.append(
            _row_cat_html(cat, [cat_row[p] for p in periods], pais_label, "cat")
        )
        subs = sub_agg[sub_agg["categoria"] == cat]
        for _, sub_row in subs.iterrows():
            rows_html.append(
                _row_cat_html(
                    sub_row["sub_categoria"],
                    [sub_row[p] for p in periods],
                    pais_label,
                    "sub",
                )
            )

    # Fila total
    total_vals = [df[p].sum() for p in periods]
    rows_html.append(_row_cat_html("TOTAL MERCADO OTC", total_vals, pais_label, "total"))

    headers = (
        f"<tr>"
        f"<th>Categoría</th><th>País</th>"
        f"<th>MAT {yr - 4}</th><th>MAT {yr - 3}</th><th>MAT {yr - 2}</th>"
        f"<th>MAT {yr - 1}</th><th>MAT {yr}</th>"
        f"<th>YoY</th><th>CAGR 3Y</th><th>CAGR 5Y</th>"
        f"</tr>"
    )

    return (
        f'<table class="shr-table">'
        f"<thead>{headers}</thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        f"</table>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# HOJA 2 — GLI (VENTAS GENOMMA)
# ─────────────────────────────────────────────────────────────────────────────

def build_gli_table(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> str:
    """
    Tabla HTML Hoja 2 — Ventas Genomma Lab.
    Jerarquía: Categoría GLI (azul oscuro) → Sub-cat (azul claro / gris sin GLI) → Total (verde).

    Solo muestra categorías con presencia GLI.
    Sub-categorías sin GLI dentro de una categoría con GLI se muestran como 'Sin presencia GLI'.

    Args:
        df:             DataFrame filtrado (universo completo — no solo GLI).
        prefix:         'mkt' o 'mkt_usd'.
        reporting_year: año correspondiente a am1.
        pais_label:     etiqueta de país para columna País.
    Returns:
        HTML string completo.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]
    yr = reporting_year

    df_gli = df[df["es_genomma"]]

    # Categorías con presencia GLI
    cat_gli_agg = (
        df_gli.groupby("categoria")[periods]
        .sum()
        .reset_index()
        .sort_values("categoria")
    )

    # Sub-categorías del universo total (para detectar ausencias)
    all_sub = (
        df.groupby(["categoria", "sub_categoria"])["mkt_am1"]
        .sum()
        .reset_index()
        .sort_values(["categoria", "sub_categoria"])
    )

    # Sub-categorías con GLI
    sub_gli_agg = (
        df_gli.groupby(["categoria", "sub_categoria"])[periods]
        .sum()
        .reset_index()
    )

    total_gli_vals = [df_gli[p].sum() for p in periods]

    rows_html: list[str] = []

    for _, cat_row in cat_gli_agg.iterrows():
        cat = cat_row["categoria"]
        rows_html.append(
            _row_cat_html(cat, [cat_row[p] for p in periods], pais_label, "cat_gli")
        )

        cat_all_subs = sorted(
            all_sub[all_sub["categoria"] == cat]["sub_categoria"].tolist()
        )
        sub_gli_cat = sub_gli_agg[sub_gli_agg["categoria"] == cat]

        for sub in cat_all_subs:
            sub_match = sub_gli_cat[sub_gli_cat["sub_categoria"] == sub]
            if sub_match.empty:
                rows_html.append(_no_gli_row_html(sub, indent=1))
            else:
                rows_html.append(
                    _row_cat_html(
                        sub,
                        [sub_match.iloc[0][p] for p in periods],
                        pais_label,
                        "sub",
                    )
                )

    rows_html.append(_row_cat_html("TOTAL GLI OTC", total_gli_vals, pais_label, "total"))

    headers = (
        f"<tr>"
        f"<th>Categoría GLI</th><th>País</th>"
        f"<th>MAT {yr - 4}</th><th>MAT {yr - 3}</th><th>MAT {yr - 2}</th>"
        f"<th>MAT {yr - 1}</th><th>MAT {yr}</th>"
        f"<th>YoY</th><th>CAGR 3Y</th><th>CAGR 5Y</th>"
        f"</tr>"
    )

    return (
        f'<table class="shr-table">'
        f"<thead>{headers}</thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        f"</table>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# HOJA 3 — SHARES GLI POR MARCA
# ─────────────────────────────────────────────────────────────────────────────

def _compute_share(gli_val: float, total_val: float) -> float:
    """
    Share individual = gli_val / total_val × 100.

    Returns:
        float porcentaje. NaN si total <= 0 o cualquier input inválido.
    """
    if pd.isna(gli_val) or pd.isna(total_val) or total_val <= 0:
        return np.nan
    return gli_val / total_val * 100


def _row_share_html(
    label: str,
    gli_vals: list,    # [gli_am5, gli_am4, gli_am3, gli_am2, gli_am1]
    tot_vals: list,    # [tot_am5, tot_am4, tot_am3, tot_am2, tot_am1]
    pais_label: str,
    row_type: str,     # "cat" | "sub" | "mrc" | "total"
) -> str:
    """
    Construye fila <tr> para la tabla de Shares GLI por Marca.
    Share = gli_val / tot_val × 100. NUNCA ms_amN.
    """
    shrs = [_compute_share(g, t) for g, t in zip(gli_vals, tot_vals)]
    s5, s4, s3, s2, s1 = shrs
    delta = (s1 - s2) if (pd.notna(s1) and pd.notna(s2)) else np.nan

    if row_type == "cat":
        bg, fg, fw, label_str = C_CAT_BG, C_CAT_FG, "700", label
    elif row_type == "sub":
        bg, fg, fw, label_str = C_SUB_BG, C_SUB_FG, "600", f"&nbsp;&nbsp;▸ {label}"
    elif row_type == "mrc":
        bg, fg, fw, label_str = C_MRC_BG, C_MRC_FG, "400", f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{label}"
    elif row_type == "total":
        bg, fg, fw, label_str = C_TOT_BG, C_TOT_FG, "700", label
    else:
        bg, fg, fw, label_str = C_NON_BG, C_NON_FG, "400", label

    pp_html = colored_pp(delta)

    return (
        f'<tr style="background:{bg};color:{fg};font-weight:{fw}">'
        f"<td>{label_str}</td>"
        f"<td>{pais_label}</td>"
        f"<td>{fmt_shr(s5)}</td>"
        f"<td>{fmt_shr(s4)}</td>"
        f"<td>{fmt_shr(s3)}</td>"
        f"<td>{fmt_shr(s2)}</td>"
        f"<td>{fmt_shr(s1)}</td>"
        f"<td>{pp_html}</td>"
        f"</tr>"
    )


def _no_gli_share_row_html(label: str, indent: int = 1) -> str:
    """Fila gris 'Sin presencia GLI' para tabla de Shares."""
    if indent == 1:
        pad = "&nbsp;&nbsp;▸ "
    elif indent == 2:
        pad = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
    else:
        pad = ""
    return (
        f'<tr style="background:{C_NON_BG};color:{C_NON_FG};font-style:italic">'
        f"<td>{pad}{label}</td>"
        f'<td colspan="7" style="text-align:left">Sin presencia GLI</td>'
        f"</tr>"
    )


def build_shares_table(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> str:
    """
    Tabla HTML Hoja 3 — Shares GLI por Marca.
    Jerarquía: Categoría (amarillo) → Sub-categoría (azul) → Marca (blanco) → Total (verde).

    REGLA: share = ventas_GLI / ventas_total. NUNCA sum(ms_amN).
    Share por marca = mkt_marca / mkt_total_subcategoría (no mkt_categoria).

    Args:
        df:             DataFrame filtrado (universo completo).
        prefix:         'mkt' o 'mkt_usd'.
        reporting_year: año correspondiente a am1.
        pais_label:     etiqueta de país para columna País.
    Returns:
        HTML string completo.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]  # am5→am1
    yr = reporting_year

    df_gli = df[df["es_genomma"]]

    # Pre-calcular agregaciones
    cat_tot  = df.groupby("categoria")[periods].sum().reset_index()
    sub_tot  = df.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index()
    cat_gli  = df_gli.groupby("categoria")[periods].sum().reset_index()
    sub_gli  = df_gli.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index()
    mrc_gli  = (
        df_gli.groupby(["categoria", "sub_categoria", "producto"])[periods]
        .sum()
        .reset_index()
        .sort_values(["categoria", "sub_categoria", "producto"])
    )
    all_sub  = (
        df.groupby(["categoria", "sub_categoria"])["mkt_am1"]
        .sum()
        .reset_index()
        .sort_values(["categoria", "sub_categoria"])
    )

    total_mkt_vals = [df[p].sum() for p in periods]
    total_gli_vals = [df_gli[p].sum() for p in periods]

    rows_html: list[str] = []

    for cat in sorted(df["categoria"].unique()):
        # ── Fila categoría ──────────────────────────────────────────────────
        cat_tot_row = cat_tot[cat_tot["categoria"] == cat]
        cat_gli_row = cat_gli[cat_gli["categoria"] == cat]

        cat_tot_v = [cat_tot_row.iloc[0][p] if not cat_tot_row.empty else np.nan for p in periods]
        cat_gli_v = [cat_gli_row.iloc[0][p] for p in periods] if not cat_gli_row.empty else [0.0] * 5

        rows_html.append(_row_share_html(cat, cat_gli_v, cat_tot_v, pais_label, "cat"))

        # ── Sub-categorías ──────────────────────────────────────────────────
        cat_subs = sorted(all_sub[all_sub["categoria"] == cat]["sub_categoria"].tolist())

        for sub in cat_subs:
            sub_tot_row = sub_tot[
                (sub_tot["categoria"] == cat) & (sub_tot["sub_categoria"] == sub)
            ]
            sub_gli_row = sub_gli[
                (sub_gli["categoria"] == cat) & (sub_gli["sub_categoria"] == sub)
            ]

            sub_tot_v = (
                [sub_tot_row.iloc[0][p] for p in periods]
                if not sub_tot_row.empty
                else [np.nan] * 5
            )

            if sub_gli_row.empty:
                rows_html.append(_no_gli_share_row_html(sub, indent=1))
                continue

            sub_gli_v = [sub_gli_row.iloc[0][p] for p in periods]
            rows_html.append(_row_share_html(sub, sub_gli_v, sub_tot_v, pais_label, "sub"))

            # ── Marcas dentro de esta sub-categoría ─────────────────────────
            marcas = mrc_gli[
                (mrc_gli["categoria"] == cat) & (mrc_gli["sub_categoria"] == sub)
            ]
            for _, mrc_row in marcas.iterrows():
                mrc_v = [mrc_row[p] for p in periods]
                # Share marca = mkt_marca / mkt_total_subcategoría
                rows_html.append(
                    _row_share_html(mrc_row["producto"], mrc_v, sub_tot_v, pais_label, "mrc")
                )

    # Fila total
    rows_html.append(
        _row_share_html("TOTAL GLI OTC", total_gli_vals, total_mkt_vals, pais_label, "total")
    )

    headers = (
        f"<tr>"
        f"<th>Categoría / Sub-categoría / Marca</th><th>País</th>"
        f"<th>SHR {yr - 4}</th><th>SHR {yr - 3}</th><th>SHR {yr - 2}</th>"
        f"<th>SHR {yr - 1}</th><th>SHR {yr}</th>"
        f"<th>Δ pp YoY</th>"
        f"</tr>"
    )

    return (
        f'<table class="shr-table">'
        f"<thead>{headers}</thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        f"</table>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# RENDERIZADORES DE TAB
# ─────────────────────────────────────────────────────────────────────────────

def tab_category(df: pd.DataFrame, prefix: str, reporting_year: int, pais_label: str) -> None:
    """Renderiza Hoja 1 — Tamaño de Categoría."""
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        return
    html = build_category_table(df, prefix, reporting_year, pais_label)
    st.markdown(html, unsafe_allow_html=True)


def tab_gli(df: pd.DataFrame, prefix: str, reporting_year: int, pais_label: str) -> None:
    """Renderiza Hoja 2 — Ventas Genomma Lab."""
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        return
    html = build_gli_table(df, prefix, reporting_year, pais_label)
    st.markdown(html, unsafe_allow_html=True)


def tab_shares(df: pd.DataFrame, prefix: str, reporting_year: int, pais_label: str) -> None:
    """Renderiza Hoja 3 — Shares GLI por Marca."""
    if df.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        return
    html = build_shares_table(df, prefix, reporting_year, pais_label)
    st.markdown(html, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT — DataFrames para Excel
# ─────────────────────────────────────────────────────────────────────────────

def _export_category_df(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> pd.DataFrame:
    """
    Construye DataFrame limpio de Tamaño de Categoría para exportar a Excel.
    Filas: categoría madre + sub-categorías + total.
    Valores numéricos sin formatear — Excel los formatea.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]
    yr = reporting_year

    cat_agg = df.groupby("categoria")[periods].sum().reset_index().sort_values("categoria")
    sub_agg = df.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index().sort_values(["categoria", "sub_categoria"])

    rows = []
    for _, cr in cat_agg.iterrows():
        v = [cr[p] for p in periods]
        rows.append({
            "Nivel": "Categoría",
            "Categoría": cr["categoria"],
            "Sub-categoría": "",
            "País": pais_label,
            f"MAT {yr-4}": v[0], f"MAT {yr-3}": v[1], f"MAT {yr-2}": v[2],
            f"MAT {yr-1}": v[3], f"MAT {yr}": v[4],
            "YoY": delta_yoy(v[4], v[3]),
            "CAGR 3Y": cagr(v[4], v[2], 3),
            "CAGR 5Y": cagr(v[4], v[0], 5),
        })
        for _, sr in sub_agg[sub_agg["categoria"] == cr["categoria"]].iterrows():
            sv = [sr[p] for p in periods]
            rows.append({
                "Nivel": "Sub-categoría",
                "Categoría": cr["categoria"],
                "Sub-categoría": sr["sub_categoria"],
                "País": pais_label,
                f"MAT {yr-4}": sv[0], f"MAT {yr-3}": sv[1], f"MAT {yr-2}": sv[2],
                f"MAT {yr-1}": sv[3], f"MAT {yr}": sv[4],
                "YoY": delta_yoy(sv[4], sv[3]),
                "CAGR 3Y": cagr(sv[4], sv[2], 3),
                "CAGR 5Y": cagr(sv[4], sv[0], 5),
            })

    tv = [df[p].sum() for p in periods]
    rows.append({
        "Nivel": "Total",
        "Categoría": "TOTAL MERCADO OTC",
        "Sub-categoría": "",
        "País": pais_label,
        f"MAT {yr-4}": tv[0], f"MAT {yr-3}": tv[1], f"MAT {yr-2}": tv[2],
        f"MAT {yr-1}": tv[3], f"MAT {yr}": tv[4],
        "YoY": delta_yoy(tv[4], tv[3]),
        "CAGR 3Y": cagr(tv[4], tv[2], 3),
        "CAGR 5Y": cagr(tv[4], tv[0], 5),
    })
    return pd.DataFrame(rows)


def _export_gli_df(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> pd.DataFrame:
    """
    Construye DataFrame limpio de Ventas GLI para exportar a Excel.
    Solo registros es_genomma=True. Incluye fila 'Sin presencia GLI' donde aplica.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]
    yr = reporting_year

    df_gli = df[df["es_genomma"]]
    cat_gli = df_gli.groupby("categoria")[periods].sum().reset_index().sort_values("categoria")
    sub_gli = df_gli.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index()
    all_sub = df.groupby(["categoria", "sub_categoria"])["mkt_am1"].sum().reset_index().sort_values(["categoria", "sub_categoria"])

    rows = []
    for _, cr in cat_gli.iterrows():
        v = [cr[p] for p in periods]
        rows.append({
            "Nivel": "Categoría GLI",
            "Categoría": cr["categoria"],
            "Sub-categoría": "",
            "País": pais_label,
            f"MAT {yr-4}": v[0], f"MAT {yr-3}": v[1], f"MAT {yr-2}": v[2],
            f"MAT {yr-1}": v[3], f"MAT {yr}": v[4],
            "YoY": delta_yoy(v[4], v[3]),
            "CAGR 3Y": cagr(v[4], v[2], 3),
            "CAGR 5Y": cagr(v[4], v[0], 5),
        })
        cat_subs = sorted(all_sub[all_sub["categoria"] == cr["categoria"]]["sub_categoria"].tolist())
        for sub in cat_subs:
            sg = sub_gli[(sub_gli["categoria"] == cr["categoria"]) & (sub_gli["sub_categoria"] == sub)]
            if sg.empty:
                rows.append({
                    "Nivel": "Sin GLI",
                    "Categoría": cr["categoria"],
                    "Sub-categoría": sub,
                    "País": pais_label,
                    f"MAT {yr-4}": None, f"MAT {yr-3}": None, f"MAT {yr-2}": None,
                    f"MAT {yr-1}": None, f"MAT {yr}": None,
                    "YoY": None, "CAGR 3Y": None, "CAGR 5Y": None,
                })
            else:
                sv = [sg.iloc[0][p] for p in periods]
                rows.append({
                    "Nivel": "Sub-categoría GLI",
                    "Categoría": cr["categoria"],
                    "Sub-categoría": sub,
                    "País": pais_label,
                    f"MAT {yr-4}": sv[0], f"MAT {yr-3}": sv[1], f"MAT {yr-2}": sv[2],
                    f"MAT {yr-1}": sv[3], f"MAT {yr}": sv[4],
                    "YoY": delta_yoy(sv[4], sv[3]),
                    "CAGR 3Y": cagr(sv[4], sv[2], 3),
                    "CAGR 5Y": cagr(sv[4], sv[0], 5),
                })

    tv = [df_gli[p].sum() for p in periods]
    rows.append({
        "Nivel": "Total GLI",
        "Categoría": "TOTAL GLI OTC",
        "Sub-categoría": "",
        "País": pais_label,
        f"MAT {yr-4}": tv[0], f"MAT {yr-3}": tv[1], f"MAT {yr-2}": tv[2],
        f"MAT {yr-1}": tv[3], f"MAT {yr}": tv[4],
        "YoY": delta_yoy(tv[4], tv[3]),
        "CAGR 3Y": cagr(tv[4], tv[2], 3),
        "CAGR 5Y": cagr(tv[4], tv[0], 5),
    })
    return pd.DataFrame(rows)


def _export_shares_df(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> pd.DataFrame:
    """
    Construye DataFrame limpio de Shares GLI por Marca para exportar a Excel.
    Share = mkt_gli / mkt_total. NUNCA ms_amN.
    """
    periods = [f"{prefix}_am{i}" for i in range(5, 0, -1)]
    yr = reporting_year

    df_gli = df[df["es_genomma"]]
    cat_tot  = df.groupby("categoria")[periods].sum().reset_index()
    sub_tot  = df.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index()
    cat_gli  = df_gli.groupby("categoria")[periods].sum().reset_index()
    sub_gli  = df_gli.groupby(["categoria", "sub_categoria"])[periods].sum().reset_index()
    mrc_gli  = df_gli.groupby(["categoria", "sub_categoria", "producto"])[periods].sum().reset_index().sort_values(["categoria", "sub_categoria", "producto"])
    all_sub  = df.groupby(["categoria", "sub_categoria"])["mkt_am1"].sum().reset_index().sort_values(["categoria", "sub_categoria"])

    total_mkt = [df[p].sum() for p in periods]
    total_gli = [df_gli[p].sum() for p in periods]

    rows = []

    for cat in sorted(df["categoria"].unique()):
        ct_row = cat_tot[cat_tot["categoria"] == cat]
        cg_row = cat_gli[cat_gli["categoria"] == cat]
        ct_v = [ct_row.iloc[0][p] if not ct_row.empty else np.nan for p in periods]
        cg_v = [cg_row.iloc[0][p] for p in periods] if not cg_row.empty else [0.0] * 5
        shrs = [_compute_share(g, t) for g, t in zip(cg_v, ct_v)]
        rows.append({
            "Nivel": "Categoría",
            "Categoría": cat, "Sub-categoría": "", "Marca": "", "País": pais_label,
            f"SHR {yr-4}": shrs[0], f"SHR {yr-3}": shrs[1], f"SHR {yr-2}": shrs[2],
            f"SHR {yr-1}": shrs[3], f"SHR {yr}": shrs[4],
            "Δ pp YoY": (shrs[4] - shrs[3]) if (pd.notna(shrs[4]) and pd.notna(shrs[3])) else np.nan,
        })

        for sub in sorted(all_sub[all_sub["categoria"] == cat]["sub_categoria"].tolist()):
            st_row = sub_tot[(sub_tot["categoria"] == cat) & (sub_tot["sub_categoria"] == sub)]
            sg_row = sub_gli[(sub_gli["categoria"] == cat) & (sub_gli["sub_categoria"] == sub)]
            st_v = [st_row.iloc[0][p] for p in periods] if not st_row.empty else [np.nan] * 5

            if sg_row.empty:
                rows.append({
                    "Nivel": "Sin GLI",
                    "Categoría": cat, "Sub-categoría": sub, "Marca": "", "País": pais_label,
                    f"SHR {yr-4}": None, f"SHR {yr-3}": None, f"SHR {yr-2}": None,
                    f"SHR {yr-1}": None, f"SHR {yr}": None, "Δ pp YoY": None,
                })
                continue

            sg_v = [sg_row.iloc[0][p] for p in periods]
            shrs = [_compute_share(g, t) for g, t in zip(sg_v, st_v)]
            rows.append({
                "Nivel": "Sub-categoría",
                "Categoría": cat, "Sub-categoría": sub, "Marca": "", "País": pais_label,
                f"SHR {yr-4}": shrs[0], f"SHR {yr-3}": shrs[1], f"SHR {yr-2}": shrs[2],
                f"SHR {yr-1}": shrs[3], f"SHR {yr}": shrs[4],
                "Δ pp YoY": (shrs[4] - shrs[3]) if (pd.notna(shrs[4]) and pd.notna(shrs[3])) else np.nan,
            })

            marcas = mrc_gli[(mrc_gli["categoria"] == cat) & (mrc_gli["sub_categoria"] == sub)]
            for _, mr in marcas.iterrows():
                mv = [mr[p] for p in periods]
                mshrs = [_compute_share(g, t) for g, t in zip(mv, st_v)]
                rows.append({
                    "Nivel": "Marca",
                    "Categoría": cat, "Sub-categoría": sub, "Marca": mr["producto"], "País": pais_label,
                    f"SHR {yr-4}": mshrs[0], f"SHR {yr-3}": mshrs[1], f"SHR {yr-2}": mshrs[2],
                    f"SHR {yr-1}": mshrs[3], f"SHR {yr}": mshrs[4],
                    "Δ pp YoY": (mshrs[4] - mshrs[3]) if (pd.notna(mshrs[4]) and pd.notna(mshrs[3])) else np.nan,
                })

    tshrs = [_compute_share(g, t) for g, t in zip(total_gli, total_mkt)]
    rows.append({
        "Nivel": "Total",
        "Categoría": "TOTAL GLI OTC", "Sub-categoría": "", "Marca": "", "País": pais_label,
        f"SHR {yr-4}": tshrs[0], f"SHR {yr-3}": tshrs[1], f"SHR {yr-2}": tshrs[2],
        f"SHR {yr-1}": tshrs[3], f"SHR {yr}": tshrs[4],
        "Δ pp YoY": (tshrs[4] - tshrs[3]) if (pd.notna(tshrs[4]) and pd.notna(tshrs[3])) else np.nan,
    })
    return pd.DataFrame(rows)


def generate_excel_bytes(
    df: pd.DataFrame,
    prefix: str,
    reporting_year: int,
    pais_label: str,
) -> bytes:
    """
    Genera un archivo Excel en memoria con 3 hojas.

    Returns:
        bytes del archivo .xlsx listo para st.download_button.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        _export_category_df(df, prefix, reporting_year, pais_label).to_excel(
            writer, sheet_name="Categoría", index=False
        )
        _export_gli_df(df, prefix, reporting_year, pais_label).to_excel(
            writer, sheet_name="GLI", index=False
        )
        _export_shares_df(df, prefix, reporting_year, pais_label).to_excel(
            writer, sheet_name="Shares GLI", index=False
        )
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Punto de entrada del dashboard."""
    st.set_page_config(
        page_title="Share Dashboard · OTC LATAM",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inyectar CSS de tablas una sola vez
    st.markdown(_TABLE_CSS, unsafe_allow_html=True)

    # Carga
    config         = load_config()
    df             = load_master()
    reporting_year = config.get("dashboard", {}).get("reporting_year", 2026)

    # Sidebar → filtros
    total_latam, paises_sel, cat_sel, subcat_sel, use_usd = sidebar_filters(df)

    # Aplicar filtros
    df_f = apply_filters(df, total_latam, paises_sel, cat_sel, subcat_sel)

    if df_f.empty:
        st.warning("Sin datos para los filtros seleccionados. Ajusta los filtros en el sidebar.")
        st.stop()

    prefix     = "mkt_usd" if use_usd else "mkt"
    pais_label = get_pais_label(total_latam, paises_sel)

    # Botón de exportación en sidebar
    excel_bytes = generate_excel_bytes(df_f, prefix, reporting_year, pais_label)
    filename = f"OTC_LATAM_{date.today():%Y-%m-%d}.xlsx"
    st.sidebar.divider()
    st.sidebar.download_button(
        label="⬇️ Exportar a Excel",
        data=excel_bytes,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    # Header con 4 KPIs
    kpis = compute_header_kpis(df_f, prefix)
    render_header(kpis, use_usd, pais_label, reporting_year)

    # 3 tabs
    tab1, tab2, tab3 = st.tabs([
        "📦 Tamaño de Categoría",
        "🏭 GLI — Ventas Genomma",
        "📊 Shares GLI por Marca",
    ])

    with tab1:
        tab_category(df_f, prefix, reporting_year, pais_label)

    with tab2:
        tab_gli(df_f, prefix, reporting_year, pais_label)

    with tab3:
        tab_shares(df_f, prefix, reporting_year, pais_label)


if __name__ == "__main__":
    main()
