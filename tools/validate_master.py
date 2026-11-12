"""Comparación determinista entre un maestro anterior y uno nuevo.

Uso:
    python tools/validate_master.py --old OLD.csv --new NEW.csv --pipeline otc|pcbev [--config config.yaml]

Exit 0 = los checks implementados aprobaron (no autoriza el reemplazo).
Exit 1 = algún check bloqueante falló. Exit 2 = error de lectura, configuración o esquema.
Solo lee archivos.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

PERIODS = range(1, 6)
VALUE_COLS = [f"{p}{i}" for p in ("mkt_am", "mkt_usd_am", "ms_am") for i in PERIODS]
EXTRA_COLS = ["es_genomma", "er_aplicado", "pais_nombre"]
PK_PCBEV = ["pais", "categoria", "laboratorio", "producto"]


class SchemaError(Exception):
    pass


def contract(pipeline: str, cfg: dict) -> tuple[list[str], dict | None]:
    """Llave y mapa de variantes del pipeline. Mapa None = control no evaluable."""
    if pipeline == "otc":
        try:
            pk = list(cfg["master_schema"]["primary_key"])
        except (KeyError, TypeError) as e:
            raise SchemaError(f"config sin master_schema.primary_key: {e}")
        return pk, dict(cfg.get("brand_consolidation") or {})
    rename = ((cfg.get("pcbev_file") or {}).get("product_corrections") or {}).get("produto_rename")
    return PK_PCBEV, (dict(rename) if rename else None)


def structural(df: pd.DataFrame, pk: list[str], label: str, pipeline: str) -> list[str]:
    if df.empty:
        return [f"{label}: maestro vacío"]
    missing = [c for c in pk + VALUE_COLS + EXTRA_COLS if c not in df.columns]
    if missing:
        return [f"{label}: faltan columnas obligatorias {missing}"]

    errors: list[str] = []
    keys = df[pk].astype("string").apply(lambda s: s.str.strip())
    empty_key = keys.isna().any(axis=1) | (keys == "").any(axis=1)
    if empty_key.any():
        errors.append(f"{label}: {int(empty_key.sum()):,} filas con llave nula o vacía")

    for c in VALUE_COLS:
        num = pd.to_numeric(df[c], errors="coerce")
        non_numeric = num.isna() & df[c].notna()
        if non_numeric.any():
            errors.append(f"{label}: {c} tiene {int(non_numeric.sum()):,} valores no numéricos")
        if np.isinf(num).any():
            errors.append(f"{label}: {c} tiene {int(np.isinf(num).sum()):,} valores infinitos")

    # mkt_am1 obligatorio, igual que en el pipeline. PC/Bev exime a Genomma
    # (process_pcbev.py: toda marca GLI se reporta aunque sea residual).
    am1_null = pd.to_numeric(df["mkt_am1"], errors="coerce").isna()
    if pipeline == "pcbev":
        is_gli = (
            df["es_genomma"].astype("string").str.strip().str.lower()
            .eq("true").fillna(False)
        )
        am1_null &= ~is_gli
    if am1_null.any():
        errors.append(f"{label}: {int(am1_null.sum()):,} filas con mkt_am1 nulo")
    return errors


def _fatal(errors: list[str]) -> bool:
    return any("faltan columnas" in e or "vacío" in e for e in errors)


def compare(old: pd.DataFrame, new: pd.DataFrame, pk: list[str], variants: dict | None, pipeline: str) -> tuple[list[str], list[str]]:
    errors = structural(new, pk, "nuevo", pipeline)
    old_errors = structural(old, pk, "anterior", pipeline)
    if _fatal(old_errors):
        raise SchemaError("; ".join(old_errors))
    info = [f"aviso {e}" for e in old_errors]
    if _fatal(errors):
        return errors, info

    info.append(f"filas: anterior={len(old):,} nuevo={len(new):,} delta={len(new) - len(old):+,}")

    dups = new[new.duplicated(subset=pk, keep=False)]
    if len(dups):
        errors.append(f"duplicados por llave en el nuevo: {len(dups):,} filas; ejemplo: {dups[pk].head(3).to_dict('records')}")
    else:
        info.append("duplicados por llave en el nuevo: 0")

    if variants is None:
        info.append("variantes sin consolidar: NO EVALUADO (falta el mapa de este pipeline en config)")
    else:
        bad = new[new["producto"].isin(variants)]
        if len(bad):
            errors.append(f"variantes sin consolidar en el nuevo: {len(bad):,} filas; ejemplos: {bad['producto'].value_counts().head(5).to_dict()}")
        else:
            info.append(f"variantes sin consolidar en el nuevo: 0 (mapa de {len(variants)} claves)")

    if not _fatal(old_errors):
        k_old = set(map(tuple, old[pk].astype(str).itertuples(index=False, name=None)))
        k_new = set(map(tuple, new[pk].astype(str).itertuples(index=False, name=None)))
        info.append(f"llaves nuevas: {len(k_new - k_old):,} | desaparecidas: {len(k_old - k_new):,} | comunes: {len(k_old & k_new):,}")

    mkt = new[[c for c in VALUE_COLS if c.startswith("mkt")]].apply(pd.to_numeric, errors="coerce")
    neg = int((mkt < 0).any(axis=1).sum())
    if neg and pipeline == "otc":
        errors.append(f"filas con valores de mercado negativos en OTC: {neg:,}")
    else:
        info.append(f"filas con valores de mercado negativos: {neg:,}" + (" (permitidos en PC/Bev)" if pipeline == "pcbev" else ""))

    if not _fatal(old_errors):
        for dim in ("pais", "categoria"):
            t = pd.concat([old.groupby(dim).size().rename("anterior"), new.groupby(dim).size().rename("nuevo")], axis=1)
            t = t.fillna(0).astype(int)
            t["delta"] = t["nuevo"] - t["anterior"]
            info.append(f"filas por {dim}:\n" + t.to_string())
    return errors, info


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--old", required=True)
    p.add_argument("--new", required=True)
    p.add_argument("--pipeline", required=True, choices=["otc", "pcbev"])
    p.add_argument("--config", default="config.yaml")
    a = p.parse_args()

    try:
        cfg = yaml.safe_load(Path(a.config).read_text(encoding="utf-8")) or {}
        pk, variants = contract(a.pipeline, cfg)
        old = pd.read_csv(a.old, low_memory=False)
        new = pd.read_csv(a.new, low_memory=False)
        errors, info = compare(old, new, pk, variants, a.pipeline)
    except Exception as e:  # lectura, config o esquema: nunca OK
        print(f"ERROR no se pudo validar: {type(e).__name__}: {e}")
        print("RESULTADO: ERROR")
        return 2

    for line in info:
        print(f"INFO  {line}")
    for line in errors:
        print(f"ERROR {line}")
    print("RESULTADO:", "BLOQUEAR" if errors else "CHECKS IMPLEMENTADOS APROBADOS (no autoriza el reemplazo)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
