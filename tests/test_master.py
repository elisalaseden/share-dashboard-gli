"""Tests de integridad de los maestros versionados y regresiones del validador.

No valida rangos de ms_am*; ese control queda fuera de este test hasta definir el
contrato por pipeline.
Requiere pytest (dependencia de pruebas, no está en requirements.txt).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from validate_master import VALUE_COLS, SchemaError, compare, contract  # noqa: E402

CFG = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
PK_OTC = ["pais", "categoria", "sub_categoria", "laboratorio", "producto"]


@pytest.mark.parametrize("rel, pipeline", [("data/master.csv", "otc"), ("data/master_pcbev.csv", "pcbev")])
def test_master_versionado(rel, pipeline):
    path = ROOT / rel
    if not path.exists():
        pytest.skip(f"{rel} no existe")
    df = pd.read_csv(path, low_memory=False)
    pk, variants = contract(pipeline, CFG)
    errors, _ = compare(df, df, pk, variants, pipeline)
    assert not errors, errors


def _base() -> pd.DataFrame:
    row = {"pais": "MEX", "categoria": "C", "sub_categoria": "S", "laboratorio": "L", "producto": "P"}
    row.update({c: 1.0 for c in VALUE_COLS})
    row.update({"es_genomma": False, "er_aplicado": 1.0, "pais_nombre": "México"})
    return pd.DataFrame([row])


def _mutate(kind: str) -> pd.DataFrame:
    df = _base()
    if kind == "sin_valores":
        return df[PK_OTC]
    if kind == "vacio":
        return df.iloc[0:0]
    if kind == "nan_am1":
        df["mkt_am1"] = np.nan
    elif kind == "inf":
        df["mkt_am1"] = np.inf
    elif kind == "producto_none":
        df["producto"] = None
    elif kind == "producto_blanco":
        df["producto"] = " "
    return df


@pytest.mark.parametrize("kind", ["sin_valores", "vacio", "nan_am1", "inf", "producto_none", "producto_blanco"])
def test_validador_rechaza_maestro_invalido(kind):
    errors, _ = compare(_base(), _mutate(kind), PK_OTC, {}, "otc")
    assert errors, f"{kind} debió bloquear"


def test_maestro_valido_pasa():
    errors, _ = compare(_base(), _base(), PK_OTC, {}, "otc")
    assert not errors, errors


@pytest.mark.parametrize(
    "es_genomma, debe_bloquear",
    [(True, False), (False, True), (None, True), (np.nan, True), (pd.NA, True)],
)
def test_pcbev_mkt_am1_nulo_solo_exento_para_genomma(es_genomma, debe_bloquear):
    pk = ["pais", "categoria", "laboratorio", "producto"]
    df = _base().drop(columns=["sub_categoria"])
    df["mkt_am1"] = np.nan
    df["es_genomma"] = es_genomma
    errors, _ = compare(df, df, pk, None, "pcbev")
    assert bool(errors) == debe_bloquear, errors


def test_variantes_pcbev_no_evaluadas_sin_mapa():
    _, variants = contract("pcbev", {"master_schema": {"primary_key": PK_OTC}})
    assert variants is None


@pytest.mark.parametrize("kind", ["sin_valores", "vacio"])
def test_anterior_no_comparable_bloquea(kind):
    with pytest.raises(SchemaError):
        compare(_mutate(kind), _base(), PK_OTC, {}, "otc")
