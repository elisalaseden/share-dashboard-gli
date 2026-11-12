# share-dashboard-gli — instrucciones para Claude Code

Pipeline mensual de shares de mercado (Genomma Lab, Consumer Insights) y dashboards Streamlit.
Python: pandas, openpyxl, pyyaml, Streamlit, plotly. Los scripts de los pipelines y dashboards están en la raíz; el validador está en `tools/`.

## Reglas que no se negocian

1. **Estos CSV no son fuente autorizada para citar shares.** `data/master.csv` y `data/master_pcbev.csv` alimentan el dashboard. No se citan como fuente de shares en análisis, decks ni respuestas.
2. **No se editan a mano** los Excel de `data/raw/` ni ningún `master*.csv`. Solo cambian corriendo el pipeline. Un hook bloquea Edit/Write sobre ellos (ver `.claude/settings.json`). El hook **no** cubre lo que se ejecuta por Bash: tampoco escribas en esos archivos con Python, `cp`, `mv`, `rm` o redirecciones, salvo los pipelines documentados abajo y solo cuando Eli lo pida.
3. Si cambia `brand_consolidation`, hay que evaluar la reconstrucción completa del maestro OTC desde las fuentes necesarias para conservar su cobertura. El upsert conserva llaves antiguas. Repetirlo con `--dry-run` no equivale a reconstruir y tampoco genera un CSV candidato. No ejecutes el borrado que sugiere `config.yaml:18–20`. Presenta un procedimiento separado que preserve el maestro vigente, genere un candidato en un destino aislado y lo valide contra el anterior con `tools/validate_master.py`. Ejecutarlo y reemplazar el maestro requieren el OK de Eli. Mientras ese procedimiento no exista, declara la reconstrucción como pendiente.
4. **Antes de escribir un maestro**, corre siempre `--dry-run` y muestra el resultado (`/pipeline-dry`). `--dry-run` no escribe el maestro, pero sí crea `logs/` y un archivo de log.
5. No hagas commit ni push sin pedido explícito.

## Comandos reales (desde la raíz)

```bash
pip install -r requirements.txt
pip install pytest   # solo para pruebas

# OTC
python run_pipeline.py --shares "data/raw/02.2026 Data OTC CloseUP Moneda local.xlsx" --fx "data/raw/2026.03_Tipos de Cambio.xlsx" --dry-run
#   opciones: --sheets MEX ARG   --no-backup   --config config.yaml

# PC / Bebidas Brasil
python run_pipeline_pcbev.py --files "data/raw/02.2026 Brasil PC_Beverages_Base Shares.xlsx" --fx "data/raw/2026.03_Tipos de Cambio.xlsx" --dry-run

# Dashboards
streamlit run app.py          # OTC, lee data/master.csv
streamlit run app_pcbev.py    # PC/Bev, lee data/master_pcbev.csv

# Validación determinista (maestro anterior vs candidato)
python tools/validate_master.py --old <csv_anterior> --new <csv_candidato> --pipeline otc|pcbev
#   exit 0 = checks implementados aprobados (no autoriza el reemplazo) · 1 = bloquear · 2 = error de lectura/config/esquema
python -m pytest tests/
```

## Mapa de archivos

| Archivo | Rol |
|---|---|
| `config.yaml` | Fuente de verdad: rutas, fingerprint del Excel, `brand_consolidation`, `master_schema` |
| `ingest.py` → `process.py` → `update_master.py` | OTC: lectura, pivot/validación/tipo de cambio, upsert a `data/master.csv` |
| `run_pipeline.py` | Orquestador OTC |
| `ingest_pcbev.py` → `process_pcbev.py` → `run_pipeline_pcbev.py` | PC/Bev Brasil, escribe `data/master_pcbev.csv` |
| `app.py`, `app_pcbev.py` | Dashboards Streamlit |
| `tools/validate_master.py`, `tests/` | Validación determinista y tests de integridad |
| `master.csv` (raíz) | Copia vieja sin consumidor en el repo. No la uses; su eliminación la decide Eli |

Llaves: OTC `pais, categoria, sub_categoria, laboratorio, producto` (`config.yaml → master_schema`); PC/Bev `pais, categoria, laboratorio, producto`.
Variantes: OTC usa `brand_consolidation`; PC/Bev usa `pcbev_file.product_corrections.produto_rename`.
Columnas: los maestros contienen valores de mercado (`mkt_am1..5`, `mkt_usd_am1..5`) y shares `ms_am1..5`. OTC recalcula los shares desde los valores; PC/Bev también consume los shares guardados. PC/Bev admite valores negativos; OTC no.

## Problemas conocidos (no los arregles sin pedido)

- Al `config.yaml` del repo le faltan `pcbev_file` y `validation_pcbev`: el pipeline PC/Bev falla con `KeyError`, y el validador marca como `NO EVALUADO` las variantes de PC/Bev.
- `app_pcbev.py` suma shares de categorías con denominadores distintos para el total.
- Los rechazos de validación (p. ej. `NULL en mkt_am1`) salen en consola y en `logs/`. Son esperados según la política, pero hay que reportarlos.
