"""
app.py — Dashboard Streamlit · Share Dashboard Investor Reporting
Ejecutar: streamlit run app.py
"""

import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from io import BytesIO

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

MASTER_PATH = Path("data/master.csv")

PAISES_LABELS = {
    "ARG": "🇦🇷 Argentina",
    "BRA": "🇧🇷 Brasil",
    "CHI": "🇨🇱 Chile",
    "COL": "🇨🇴 Colombia",
    "ECU": "🇪🇨 Ecuador",
    "MEX": "🇲🇽 México",
    "PER": "🇵🇪 Perú",
}

AM_TO_YEAR = {1: "2026", 2: "2025", 3: "2024", 4: "2023", 5: "2022"}


# ---------------------------------------------------------------------------
# CARGA
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_master() -> pd.DataFrame:
    if not MASTER_PATH.exists():
        st.error(f"master.csv no encontrado en {MASTER_PATH.absolute()}.")
        st.stop()
    return pd.read_csv(MASTER_PATH, low_memory=False)


# ---------------------------------------------------------------------------
# CÁLCULOS
# ---------------------------------------------------------------------------

def cagr(vf, vi, n):
    if pd.isna(vf) or pd.isna(vi) or vi <= 0 or n <= 0:
        return np.nan
    return (vf / vi) ** (1 / n) - 1


def delta_yoy(vn, vo):
    if pd.isna(vn) or pd.isna(vo) or vo == 0:
        return np.nan
    return (vn / vo) - 1


def fmt_mill(v, usd=False):
    if pd.isna(v):
        return "—"
    sym = "USD " if usd else ""
    if abs(v) >= 1e9:
        return f"{sym}{v/1e9:.1f}B"
    elif abs(v) >= 1e6:
        return f"{sym}{v/1e6:.1f}M"
    elif abs(v) >= 1e3:
        return f"{sym}{v/1e3:.1f}K"
    return f"{sym}{v:,.0f}"


def fmt_pct(v, decimals=1):
    if pd.isna(v):
        return "—"
    return f"{v*100:.{decimals}f}%"


def fmt_pp(v):
    if pd.isna(v):
        return "—"
    return f"{v:+.2f} pp"


def to_excel(sheets: dict) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# ACORDEÓN — TABLA JERÁRQUICA
# ---------------------------------------------------------------------------

def build_acordeon(
    df: pd.DataFrame,
    prefix: str,
    solo_gli: bool,
    total_latam: bool,
    use_usd: bool,
) -> list[dict]:
    """
    Construye lista de filas jerárquicas:
    - Fila CATEGORÍA MADRE (negrita) = suma subcategorías
    - Fila SUBCATEGORÍA (indentada) = valor directo
    - Fila TOTAL al pie
    Orden columnas: Categoría | País | MAT 2022..2026 | YoY | CAGR 3Y | CAGR 5Y
    """
    if solo_gli:
        df = df[df["es_genomma"] == True].copy()

    pais_label = "Total LATAM" if total_latam else None
    group_cat    = ["categoria"]
    group_subcat = ["categoria", "sub_categoria"]

    if not total_latam:
        group_cat    = ["categoria", "pais", "pais_nombre"]
        group_subcat = ["categoria", "sub_categoria", "pais", "pais_nombre"]

    agg_dict = {
        f"{prefix}_am{i}": "sum"
        for i in range(1, 6)
        if f"{prefix}_am{i}" in df.columns
    }

    if not agg_dict:
        return []

    cat_agg    = df.groupby(group_cat).agg(agg_dict).reset_index()
    subcat_agg = df.groupby(group_subcat).agg(agg_dict).reset_index()

    def make_row(r, label, nivel, pais_lbl):
        am = {yr: r.get(f"{prefix}_am{i}", np.nan) for i, yr in AM_TO_YEAR.items()}
        return {
            "_nivel":   nivel,
            "_label":   label,
            "Categoría": ("▸ " if nivel == "sub" else "") + label,
            "País":      pais_lbl,
            "MAT 2022":  am["2022"],
            "MAT 2023":  am["2023"],
            "MAT 2024":  am["2024"],
            "MAT 2025":  am["2025"],
            "MAT 2026":  am["2026"],
            "YoY":       delta_yoy(am["2026"], am["2025"]),
            "CAGR 3Y":   cagr(am["2026"], am["2024"], 3),
            "CAGR 5Y":   cagr(am["2026"], am["2022"], 5),
        }

    rows = []
    cats_sorted = sorted(cat_agg["categoria"].unique())

    for cat in cats_sorted:
        # Fila categoría madre
        if total_latam:
            r_cat = cat_agg[cat_agg["categoria"] == cat].iloc[0]
            rows.append(make_row(r_cat, cat, "cat", "Total LATAM"))
            # Subcategorías
            subs = subcat_agg[subcat_agg["categoria"] == cat].sort_values("sub_categoria")
            for _, r_sub in subs.iterrows():
                rows.append(make_row(r_sub, r_sub["sub_categoria"], "sub", "Total LATAM"))
        else:
            paises = sorted(cat_agg[cat_agg["categoria"] == cat]["pais"].unique())
            for pais in paises:
                r_cat = cat_agg[
                    (cat_agg["categoria"] == cat) & (cat_agg["pais"] == pais)
                ].iloc[0]
                pais_lbl = PAISES_LABELS.get(pais, pais)
                rows.append(make_row(r_cat, cat, "cat", pais_lbl))
                subs = subcat_agg[
                    (subcat_agg["categoria"] == cat) & (subcat_agg["pais"] == pais)
                ].sort_values("sub_categoria")
                for _, r_sub in subs.iterrows():
                    rows.append(make_row(r_sub, r_sub["sub_categoria"], "sub", pais_lbl))

    # Fila TOTAL
    mat_cols = ["MAT 2022", "MAT 2023", "MAT 2024", "MAT 2025", "MAT 2026"]
    cat_rows = [r for r in rows if r["_nivel"] == "cat"]
    totals   = {c: sum(r[c] for r in cat_rows if not pd.isna(r[c])) for c in mat_cols}
    rows.append({
        "_nivel":    "total",
        "_label":    "TOTAL",
        "Categoría": "TOTAL MERCADO OTC",
        "País":      "Total LATAM" if total_latam else "Todos",
        "MAT 2022":  totals["MAT 2022"],
        "MAT 2023":  totals["MAT 2023"],
        "MAT 2024":  totals["MAT 2024"],
        "MAT 2025":  totals["MAT 2025"],
        "MAT 2026":  totals["MAT 2026"],
        "YoY":       delta_yoy(totals["MAT 2026"], totals["MAT 2025"]),
        "CAGR 3Y":   cagr(totals["MAT 2026"], totals["MAT 2024"], 3),
        "CAGR 5Y":   cagr(totals["MAT 2026"], totals["MAT 2022"], 5),
    })

    return rows


def build_share_acordeon(
    df: pd.DataFrame,
    solo_gli: bool,
    total_latam: bool,
) -> list[dict]:
    """
    Acordeón de shares. Si solo_gli: share GLI = sum(mkt_gli) / sum(mkt_cat).
    Si no: ms_amN directo (promedio de productos).
    """
    pais_label   = "Total LATAM" if total_latam else None
    group_cat    = ["categoria"]
    group_subcat = ["categoria", "sub_categoria"]
    if not total_latam:
        group_cat    = ["categoria", "pais", "pais_nombre"]
        group_subcat = ["categoria", "sub_categoria", "pais", "pais_nombre"]

    def share_row(df_sub, label, nivel, pais_lbl, cat_df=None):
        shr = {}
        for i, yr in AM_TO_YEAR.items():
            if solo_gli:
                gli_v = df_sub[df_sub["es_genomma"] == True][f"mkt_am{i}"].sum() if f"mkt_am{i}" in df_sub.columns else np.nan
                cat_v = df_sub[f"mkt_am{i}"].sum() if f"mkt_am{i}" in df_sub.columns else np.nan
                shr[yr] = (gli_v / cat_v * 100) if cat_v and cat_v > 0 else np.nan
            else:
                col = f"ms_am{i}"
                shr[yr] = df_sub[col].mean() if col in df_sub.columns else np.nan

        return {
            "_nivel":    nivel,
            "_label":    label,
            "Categoría": ("▸ " if nivel == "sub" else "") + label,
            "País":       pais_lbl,
            "SHR 2022":  shr["2022"],
            "SHR 2023":  shr["2023"],
            "SHR 2024":  shr["2024"],
            "SHR 2025":  shr["2025"],
            "SHR 2026":  shr["2026"],
            "Δ pp YoY":  (shr["2026"] - shr["2025"])
                          if not (pd.isna(shr["2026"]) or pd.isna(shr["2025"])) else np.nan,
        }

    shr_cols_check = ["SHR 2022", "SHR 2023", "SHR 2024", "SHR 2025", "SHR 2026"]

    def has_share(r):
        return any((not pd.isna(r[c])) and r[c] > 0 for c in shr_cols_check)

    rows = []
    cats_sorted = sorted(df["categoria"].unique())

    for cat in cats_sorted:
        df_cat = df[df["categoria"] == cat]
        if total_latam:
            rows.append(share_row(df_cat, cat, "cat", "Total LATAM"))
            for sub in sorted(df_cat["sub_categoria"].unique()):
                df_sub = df_cat[df_cat["sub_categoria"] == sub]
                sub_row = share_row(df_sub, sub, "sub", "Total LATAM")
                if has_share(sub_row):
                    rows.append(sub_row)
        else:
            for pais in sorted(df_cat["pais"].unique()):
                df_pais = df_cat[df_cat["pais"] == pais]
                pais_lbl = PAISES_LABELS.get(pais, pais)
                rows.append(share_row(df_pais, cat, "cat", pais_lbl))
                for sub in sorted(df_pais["sub_categoria"].unique()):
                    df_sub = df_pais[df_pais["sub_categoria"] == sub]
                    sub_row = share_row(df_sub, sub, "sub", pais_lbl)
                    if has_share(sub_row):
                        rows.append(sub_row)

    # Fila TOTAL — promedio de categorías madre
    cat_rows = [r for r in rows if r["_nivel"] == "cat"]
    shr_cols = ["SHR 2022", "SHR 2023", "SHR 2024", "SHR 2025", "SHR 2026"]
    tot = {c: np.nanmean([r[c] for r in cat_rows]) for c in shr_cols}
    rows.append({
        "_nivel":    "total",
        "_label":    "TOTAL",
        "Categoría": "TOTAL MERCADO OTC",
        "País":      "Total LATAM" if total_latam else "Todos",
        **tot,
        "Δ pp YoY":  (tot["SHR 2026"] - tot["SHR 2025"])
                      if not (pd.isna(tot["SHR 2026"]) or pd.isna(tot["SHR 2025"])) else np.nan,
    })

    return rows


# ---------------------------------------------------------------------------
# RENDER TABLA
# ---------------------------------------------------------------------------

def render_market(rows: list[dict], use_usd: bool) -> pd.DataFrame:
    display = []
    for r in rows:
        display.append({
            "Categoría": r["Categoría"],
            "País":      r["País"],
            "MAT 2022":  fmt_mill(r["MAT 2022"], usd=use_usd),
            "MAT 2023":  fmt_mill(r["MAT 2023"], usd=use_usd),
            "MAT 2024":  fmt_mill(r["MAT 2024"], usd=use_usd),
            "MAT 2025":  fmt_mill(r["MAT 2025"], usd=use_usd),
            "MAT 2026":  fmt_mill(r["MAT 2026"], usd=use_usd),
            "YoY":       fmt_pct(r["YoY"]),
            "CAGR 3Y":   fmt_pct(r["CAGR 3Y"]),
            "CAGR 5Y":   fmt_pct(r["CAGR 5Y"]),
        })
    return pd.DataFrame(display)


def render_share(rows: list[dict]) -> pd.DataFrame:
    display = []
    for r in rows:
        display.append({
            "Categoría": r["Categoría"],
            "País":      r["País"],
            "SHR 2022":  fmt_pct(r["SHR 2022"] / 100) if not pd.isna(r["SHR 2022"]) else "—",
            "SHR 2023":  fmt_pct(r["SHR 2023"] / 100) if not pd.isna(r["SHR 2023"]) else "—",
            "SHR 2024":  fmt_pct(r["SHR 2024"] / 100) if not pd.isna(r["SHR 2024"]) else "—",
            "SHR 2025":  fmt_pct(r["SHR 2025"] / 100) if not pd.isna(r["SHR 2025"]) else "—",
            "SHR 2026":  fmt_pct(r["SHR 2026"] / 100) if not pd.isna(r["SHR 2026"]) else "—",
            "Δ pp YoY":  fmt_pp(r["Δ pp YoY"]),
        })
    return pd.DataFrame(display)


def rows_to_export(rows: list[dict]) -> pd.DataFrame:
    keep = [k for k in rows[0].keys() if not k.startswith("_")]
    return pd.DataFrame([{k: r[k] for k in keep} for r in rows])


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="Share Dashboard · OTC LATAM",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📊 Share Dashboard — OTC LATAM · Investor Reporting")
    st.caption("Genomma Lab Internacional · Consumer Insights")

    df = load_master()

    # ── SIDEBAR ──────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Filtros")

        total_latam = st.toggle("🌎 Total LATAM", value=True)

        paises_disp = sorted(df["pais"].unique())
        if total_latam:
            pais_sel = paises_disp
            st.multiselect("País", options=paises_disp,
                           format_func=lambda x: PAISES_LABELS.get(x, x),
                           default=paises_disp, disabled=True)
        else:
            pais_sel = st.multiselect(
                "País", options=paises_disp,
                format_func=lambda x: PAISES_LABELS.get(x, x),
                default=paises_disp,
            )

        cats_disp = sorted(df["categoria"].unique())
        cat_sel = st.multiselect("Categoría Madre", options=cats_disp, default=cats_disp)

        st.divider()
        use_usd = st.toggle("Mostrar en USD", value=True)
        st.caption("USD: tipo de cambio oficial Genomma.")

        st.divider()
        st.caption(
            f"**Dataset:**\n"
            f"- {len(df):,} registros\n"
            f"- {df['pais'].nunique()} países\n"
            f"- {df['categoria'].nunique()} categorías\n"
            f"- {df['producto'].nunique():,} productos"
        )

    # ── FILTROS ───────────────────────────────────────────────────────────
    df_f = df.copy()
    if pais_sel:
        df_f = df_f[df_f["pais"].isin(pais_sel)]
    if cat_sel:
        df_f = df_f[df_f["categoria"].isin(cat_sel)]

    if df_f.empty:
        st.warning("Sin datos para los filtros seleccionados.")
        st.stop()

    prefix = "mkt_usd" if use_usd else "mkt"

    # ── KPIs HEADER ───────────────────────────────────────────────────────
    col_am1 = f"{prefix}_am1"
    col_am5 = f"{prefix}_am5"
    total_mkt = df_f[col_am1].sum() if col_am1 in df_f.columns else np.nan
    total_gli = df_f[df_f["es_genomma"] == True][col_am1].sum() if col_am1 in df_f.columns else np.nan
    share_gli = (total_gli / total_mkt * 100) if total_mkt > 0 else np.nan
    cagr5_tot = cagr(
        df_f[col_am1].sum() if col_am1 in df_f.columns else np.nan,
        df_f[col_am5].sum() if col_am5 in df_f.columns else np.nan, 5
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Mercado Total MAT 2026", fmt_mill(total_mkt, usd=use_usd))
    k2.metric("Venta GLI MAT 2026",     fmt_mill(total_gli, usd=use_usd))
    k3.metric("Share GLI Total",        f"{share_gli:.2f}%" if not pd.isna(share_gli) else "—")
    k4.metric("CAGR Mercado 5Y",        fmt_pct(cagr5_tot))

    st.divider()

    # ── TABS ─────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs([
        "📦 Tamaño de Categoría",
        "🏭 Performance GLI",
        "📊 Shares GLI",
    ])

    # ── TAB 1: TAMAÑO DE CATEGORÍA ────────────────────────────────────────
    with tab1:
        st.subheader("📦 Tamaño de Categoría — Acordeón")
        rows_cat = build_acordeon(df_f, prefix, solo_gli=False,
                                  total_latam=total_latam, use_usd=use_usd)
        if not rows_cat:
            st.warning("Sin datos.")
        else:
            st.dataframe(render_market(rows_cat, use_usd),
                         use_container_width=True, hide_index=True)

    # ── TAB 2: PERFORMANCE GLI ────────────────────────────────────────────
    with tab2:
        st.subheader("🏭 Performance Genomma Lab — Acordeón")
        rows_gli = build_acordeon(df_f, prefix, solo_gli=True,
                                  total_latam=total_latam, use_usd=use_usd)
        if not rows_gli:
            st.warning("Sin datos GLI.")
        else:
            st.dataframe(render_market(rows_gli, use_usd),
                         use_container_width=True, hide_index=True)

    # ── TAB 3: SHARES GLI ────────────────────────────────────────────────
    with tab3:
        st.subheader("📊 Market Share GLI — Acordeón")
        rows_shr = build_share_acordeon(df_f, solo_gli=True, total_latam=total_latam)
        if not rows_shr:
            st.warning("Sin datos de share.")
        else:
            st.dataframe(render_share(rows_shr),
                         use_container_width=True, hide_index=True)

    # ── EXPORTAR EXCEL ────────────────────────────────────────────────────
    st.divider()
    if st.button("⬇ Generar Excel — Categoría + GLI + Shares"):
        excel_bytes = to_excel({
            "Categoría":  rows_to_export(rows_cat),
            "GLI":        rows_to_export(rows_gli),
            "Shares GLI": rows_to_export(rows_shr),
        })
        st.download_button(
            label="📥 Descargar Excel",
            data=excel_bytes,
            file_name="share_dashboard_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()
