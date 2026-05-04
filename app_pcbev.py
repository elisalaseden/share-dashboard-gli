"""
app_pcbev.py — Dashboard Streamlit · PC y Bebidas Brasil
Share Dashboard · Investor Reporting Pipeline

Vistas: PC (Personal Care) / BEBIDAS
CAGR: 4Y y 2Y (60 meses de historia disponibles)
Sin sub-categoría (proveedores no estandarizados)
País: Brasil exclusivamente

Ejecutar:
    python -m streamlit run app_pcbev.py
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime as _dt


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

MASTER_PCBEV_PATH = Path("data/master_pcbev.csv")

COLOR_GLI     = "#1B4F72"
COLOR_CAT     = "#AED6F1"

CAGR_CFG = {
    "PC":      {"long": {"years": 4, "col": "am5", "label": "CAGR 4Y"},
                "mid":  {"years": 2, "col": "am3", "label": "CAGR 2Y"}},
    "BEBIDAS": {"long": {"years": 4, "col": "am5", "label": "CAGR 4Y"},
                "mid":  {"years": 2, "col": "am3", "label": "CAGR 2Y"}},
}


# ---------------------------------------------------------------------------
# CARGA
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_master_pcbev() -> pd.DataFrame:
    if not MASTER_PCBEV_PATH.exists():
        st.error(f"master_pcbev.csv no encontrado. Ejecuta run_pipeline_pcbev.py primero.")
        st.stop()
    return pd.read_csv(MASTER_PCBEV_PATH, low_memory=False)


# ---------------------------------------------------------------------------
# CÁLCULOS
# ---------------------------------------------------------------------------

def cagr(vf: float, vi: float, años: int) -> float:
    if pd.isna(vf) or pd.isna(vi) or vi <= 0 or años <= 0:
        return np.nan
    return (vf / vi) ** (1 / años) - 1


def delta_yoy(vn: float, vo: float) -> float:
    if pd.isna(vn) or pd.isna(vo) or vo == 0:
        return np.nan
    return (vn / vo) - 1


def compute_category_metrics(df: pd.DataFrame, use_usd: bool, ccfg: dict) -> pd.DataFrame:
    prefix = "mkt_usd" if use_usd else "mkt"
    cols = {f"{prefix}_am{i}": f"am{i}" for i in range(1, 6)}
    agg = {col: "sum" for col in cols if col in df.columns}
    if not agg:
        return pd.DataFrame()
    grp = df.groupby(["pais", "pais_nombre", "categoria"]).agg(agg).reset_index()
    grp.rename(columns=cols, inplace=True)
    long_y = ccfg["long"]["years"]
    mid_y  = ccfg["mid"]["years"]
    long_c = ccfg["long"]["col"]
    mid_c  = ccfg["mid"]["col"]
    grp["cagr_long"]  = grp.apply(lambda r: cagr(r.get("am1", np.nan), r.get(long_c, np.nan), long_y), axis=1)
    grp["cagr_mid"]   = grp.apply(lambda r: cagr(r.get("am1", np.nan), r.get(mid_c, np.nan), mid_y), axis=1)
    grp["delta_yoy"]  = grp.apply(lambda r: delta_yoy(r.get("am1", np.nan), r.get("am2", np.nan)), axis=1)
    return grp


# ---------------------------------------------------------------------------
# FORMATEADORES
# ---------------------------------------------------------------------------

def fmt_millions(val: float, usd: bool = False) -> str:
    if pd.isna(val):
        return "—"
    sym = "USD " if usd else "BRL "
    if abs(val) >= 1e9:
        return f"{sym}{val/1e9:.1f}B"
    elif abs(val) >= 1e6:
        return f"{sym}{val/1e6:.1f}M"
    return f"{sym}{val:,.0f}"

def fmt_pct(v: float) -> str:
    return f"{v*100:.1f}%" if not pd.isna(v) else "—"

def fmt_share(v: float) -> str:
    return f"{v:.2f}%" if not pd.isna(v) else "—"

def fmt_delta_pp(v: float) -> str:
    return f"{v:+.2f} pp" if not pd.isna(v) else "—"


# ---------------------------------------------------------------------------
# SECCIÓN 1 — TAMAÑO DE CATEGORÍA
# ---------------------------------------------------------------------------

def section_category_size(df: pd.DataFrame, use_usd: bool, ccfg: dict) -> None:
    st.subheader("📊 Tamaño de Categoría")

    metrics = compute_category_metrics(df, use_usd, ccfg)
    if metrics.empty:
        st.warning("Sin datos.")
        return

    yr = _dt.now().year
    mat = {f"am{i}": f"MAT {yr - i + 1}" for i in range(1, 6)}
    long_lbl = ccfg["long"]["label"]
    mid_lbl  = ccfg["mid"]["label"]

    display = {
        "categoria":   "Categoría",
        "pais_nombre": "País",
        "am1": mat["am1"], "am2": mat["am2"], "am3": mat["am3"],
        "am4": mat["am4"], "am5": mat["am5"],
        "delta_yoy":  "Δ YoY",
        "cagr_mid":   mid_lbl,
        "cagr_long":  long_lbl,
    }
    df_d = metrics[[c for c in display if c in metrics.columns]].rename(columns=display).copy()
    for col in [mat[f"am{i}"] for i in range(1, 6)]:
        if col in df_d.columns:
            df_d[col] = df_d[col].apply(lambda v: fmt_millions(v, usd=use_usd))
    for col in ["Δ YoY", mid_lbl, long_lbl]:
        if col in df_d.columns:
            df_d[col] = df_d[col].apply(lambda v: f"{v*100:.1f}%" if not pd.isna(v) else "—")

    st.dataframe(df_d, use_container_width=True, hide_index=True)

    fig = px.bar(
        metrics.sort_values("am1", ascending=False),
        x="categoria", y="am1", color="pais_nombre", barmode="group",
        title=f"MAT Actual ({mat['am1']}) — {'USD' if use_usd else 'BRL'}",
        labels={"am1": "USD" if use_usd else "BRL", "categoria": "Categoría"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# SECCIÓN 2 — CAGR
# ---------------------------------------------------------------------------

def section_cagr(df: pd.DataFrame, use_usd: bool, ccfg: dict) -> None:
    st.subheader("📈 CAGR por Categoría")

    metrics = compute_category_metrics(df, use_usd, ccfg)
    if metrics.empty:
        return

    long_lbl = ccfg["long"]["label"]
    mid_lbl  = ccfg["mid"]["label"]

    fig = go.Figure()
    for _, row in metrics.iterrows():
        fig.add_trace(go.Bar(
            name=f"{row['categoria']}",
            x=[long_lbl, mid_lbl, "Δ YoY"],
            y=[
                row.get("cagr_long", 0) * 100 if not pd.isna(row.get("cagr_long")) else 0,
                row.get("cagr_mid",  0) * 100 if not pd.isna(row.get("cagr_mid"))  else 0,
                row.get("delta_yoy", 0) * 100 if not pd.isna(row.get("delta_yoy")) else 0,
            ],
        ))
    fig.update_layout(
        barmode="group",
        title=f"Tasas de Crecimiento — {long_lbl} / {mid_lbl} / Δ YoY",
        yaxis_ticksuffix="%",
    )
    st.plotly_chart(fig, use_container_width=True)

    cols = st.columns(3)
    first = metrics.iloc[0] if len(metrics) > 0 else None
    if first is not None:
        with cols[0]:
            st.metric(long_lbl, fmt_pct(first.get("cagr_long", np.nan)))
        with cols[1]:
            st.metric(mid_lbl,  fmt_pct(first.get("cagr_mid",  np.nan)))
        with cols[2]:
            st.metric("Δ YoY",  fmt_pct(first.get("delta_yoy", np.nan)))


# ---------------------------------------------------------------------------
# SECCIÓN 3 — SHARES GLI POR MARCA
# ---------------------------------------------------------------------------

def section_share_por_marca(df: pd.DataFrame) -> None:
    """
    Acordeón: Total categorías → Categoría (share GLI) → Marca.
    PC/Bev no tiene sub-categoría estandarizada.
    """
    st.subheader("📊 Market Share GLI por Marca")

    df_gli = df[df["es_genomma"] == True].copy()
    if df_gli.empty:
        st.warning("Sin datos GLI.")
        return

    yr = _dt.now().year
    ms_cols   = [f"ms_am{i}" for i in range(1, 6)]
    yr_labels = {f"ms_am{i}": f"SHR {yr - i + 1}" for i in range(1, 6)}

    def fmt_s(v):
        return f"{v:.2f}%" if not pd.isna(v) else "—"

    def build_row(label, src):
        row = {"Categoría / Marca": label}
        for col in ms_cols:
            val = src[col].sum() if col in src.columns else np.nan
            row[yr_labels[col]] = fmt_s(val)
        am1 = src["ms_am1"].sum() if "ms_am1" in src.columns else np.nan
        am2 = src["ms_am2"].sum() if "ms_am2" in src.columns else np.nan
        row["Δ pp YoY"] = f"{am1-am2:+.2f} pp" if not (pd.isna(am1) or pd.isna(am2)) else "—"
        return row

    def build_brand(label, brand):
        row = {"Categoría / Marca": label}
        for col in ms_cols:
            row[yr_labels[col]] = fmt_s(brand.get(col, np.nan))
        am1 = brand.get("ms_am1", np.nan)
        am2 = brand.get("ms_am2", np.nan)
        row["Δ pp YoY"] = f"{am1-am2:+.2f} pp" if not (pd.isna(am1) or pd.isna(am2)) else "—"
        return row

    # Total general
    st.dataframe(
        pd.DataFrame([build_row("🌎 TOTAL TODAS LAS CATEGORÍAS", df_gli)]),
        use_container_width=True, hide_index=True,
    )
    st.divider()

    for cat in sorted(df_gli["categoria"].unique()):
        df_cat = df_gli[df_gli["categoria"] == cat]
        am1_cat = df_cat["ms_am1"].sum() if "ms_am1" in df_cat.columns else np.nan
        header = f"**{cat}** — Share GLI AM1: {fmt_s(am1_cat)}"

        with st.expander(header, expanded=False):
            rows = [build_row(f"TOTAL {cat}", df_cat)]
            for _, brand in df_cat.sort_values("ms_am1", ascending=False, na_position="last").iterrows():
                rows.append(build_brand(f"  {brand['producto']}", brand))
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# SECCIÓN 4 — SHARE EVOLUTION
# ---------------------------------------------------------------------------

def section_share_evolution(df: pd.DataFrame) -> None:
    st.subheader("📉 Evolución de Market Share GLI")

    df_gli = df[df["es_genomma"] == True].copy()
    if df_gli.empty:
        return

    yr = _dt.now().year
    periods    = ["am5", "am4", "am3", "am2", "am1"]
    per_labels = [f"MAT {yr - 4 + i}" for i in range(5)]

    # Agregar por categoría
    ms_cols = [f"ms_am{i}" for i in range(1, 6)]
    cat_agg = df_gli.groupby("categoria")[ms_cols].sum().reset_index()

    fig = go.Figure()
    for _, r in cat_agg.iterrows():
        shares = [r.get(f"ms_{p}", np.nan) for p in periods]
        fig.add_trace(go.Scatter(
            x=per_labels, y=shares,
            mode="lines+markers", name=r["categoria"], line=dict(width=2),
        ))
    fig.update_layout(
        title="Share of Market GLI por Período (%)",
        yaxis_title="Market Share (%)", yaxis_ticksuffix="%",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Tabla Δ pp
    delta_rows = []
    for _, r in cat_agg.iterrows():
        am1 = r.get("ms_am1", np.nan)
        am2 = r.get("ms_am2", np.nan)
        delta_rows.append({
            "Categoría":      r["categoria"],
            f"Share {yr} (%)":   fmt_share(am1),
            f"Share {yr-1} (%)": fmt_share(am2),
            "Δ Puntos Share":    fmt_delta_pp(am1 - am2) if not (pd.isna(am1) or pd.isna(am2)) else "—",
        })
    st.dataframe(pd.DataFrame(delta_rows), use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="Share Dashboard · PC & Bebidas Brasil",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📊 Share Dashboard — PC & Bebidas · Brasil")
    st.caption("Genomma Lab Internacional · Consumer Insights")

    df_all = load_master_pcbev()

    with st.sidebar:
        st.header("⚙️ Filtros")

        pipeline = st.radio("Vista", ["PC", "BEBIDAS"], horizontal=True)
        st.divider()

        # Filtrar por vista
        if pipeline == "PC":
            df = df_all[df_all["categoria"] != "BEBIDAS"].copy()
        else:
            df = df_all[df_all["categoria"] == "BEBIDAS"].copy()

        ccfg = CAGR_CFG[pipeline]

        cats_disp = sorted(df["categoria"].unique())
        cat_sel = st.multiselect("Categoría", options=cats_disp, default=cats_disp)

        st.divider()
        use_usd = st.toggle("Mostrar en USD", value=False)
        st.caption("USD: tipo de cambio oficial Genomma.\nLocal: Reais (BRL).")
        st.divider()

        st.caption(
            f"**Vista:** {pipeline} · Brasil\n\n"
            f"- {len(df):,} registros\n"
            f"- {df['categoria'].nunique()} categorías\n"
            f"- {df['producto'].nunique():,} productos\n"
        )

    # Aplicar filtros
    df_f = df[df["categoria"].isin(cat_sel)].copy() if cat_sel else df.copy()

    if df_f.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        st.stop()

    # KPIs
    prefix  = "mkt_usd" if use_usd else "mkt"
    col_am1 = f"{prefix}_am1"
    col_amL = f"{prefix}_{ccfg['long']['col']}"
    yr      = _dt.now().year

    total_mkt  = df_f[col_am1].sum() if col_am1 in df_f.columns else np.nan
    total_gli  = df_f[df_f["es_genomma"] == True][col_am1].sum() if col_am1 in df_f.columns else np.nan
    share_gli  = (total_gli / total_mkt * 100) if (not pd.isna(total_mkt) and total_mkt > 0) else np.nan
    cagr_long  = cagr(
        df_f[col_am1].sum() if col_am1 in df_f.columns else np.nan,
        df_f[col_amL].sum() if col_amL in df_f.columns else np.nan,
        ccfg["long"]["years"],
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"Mercado Total MAT {yr}", fmt_millions(total_mkt, usd=use_usd))
    k2.metric(f"Venta GLI MAT {yr}",     fmt_millions(total_gli, usd=use_usd))
    k3.metric("Share GLI Total",          f"{share_gli:.2f}%" if not pd.isna(share_gli) else "—")
    k4.metric(ccfg["long"]["label"] + " Mercado", fmt_pct(cagr_long))

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "📦 Tamaño de Categoría",
        "📈 CAGR",
        "📊 Shares GLI",
        "📉 Share Evolution",
    ])

    with tab1:
        section_category_size(df_f, use_usd, ccfg)
    with tab2:
        section_cagr(df_f, use_usd, ccfg)
    with tab3:
        section_share_por_marca(df_f)
    with tab4:
        section_share_evolution(df_f)


if __name__ == "__main__":
    main()
