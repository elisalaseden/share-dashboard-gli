# Share Dashboard · OTC LATAM · Investor Reporting Pipeline
Genomma Lab Internacional — Consumer Insights

---

## ESTRUCTURA DEL PROYECTO

```
share-dashboard-gli/
├── data/
│   ├── raw/                 → Excel originales (shares + tipo de cambio)
│   ├── master.csv           → maestro OTC (lo lee app.py)
│   └── master_pcbev.csv     → maestro PC/Bev Brasil (lo lee app_pcbev.py)
├── ingest.py, process.py, update_master.py, run_pipeline.py        → pipeline OTC
├── ingest_pcbev.py, process_pcbev.py, run_pipeline_pcbev.py        → pipeline PC/Bev
├── app.py, app_pcbev.py → dashboards Streamlit
├── tools/validate_master.py → validación maestro anterior vs candidato
├── tests/               → tests de integridad (pytest)
├── config.yaml          → fuente de verdad del pipeline
├── CLAUDE.md            → reglas para Claude Code
└── requirements.txt
```

---

## INSTALACIÓN (una sola vez)

Windows (PowerShell o cmd):

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
pip install pytest
```

`pytest` solo se usa para las pruebas; no está en `requirements.txt`.

---

## USO MENSUAL — PIPELINE COMPLETO

Desde la raíz del proyecto, con el entorno activado (`venv\Scripts\activate`):

```
python run_pipeline.py --shares "data/raw/02.2026 Data OTC CloseUP Moneda local.xlsx" --fx "data/raw/2026.03_Tipos de Cambio.xlsx"
```

Con dry-run: no escribe el maestro; el orquestador sí crea logs.

```
python run_pipeline.py --shares "data/raw/02.2026 Data OTC CloseUP Moneda local.xlsx" --fx "data/raw/2026.03_Tipos de Cambio.xlsx" --dry-run
```

Solo algunos países (para pruebas):

```
python run_pipeline.py --shares "..." --fx "..." --sheets MEX ARG
```

PC / Bebidas Brasil:

```
python run_pipeline_pcbev.py --files "data/raw/02.2026 Brasil PC_Beverages_Base Shares.xlsx" --fx "data/raw/2026.03_Tipos de Cambio.xlsx" --dry-run
```

---

## LEVANTAR EL DASHBOARD

```
streamlit run app.py
streamlit run app_pcbev.py
```

El dashboard abre en: http://localhost:8501

---

## VALIDACIÓN Y PRUEBAS

```
python tools/validate_master.py --old <csv_anterior> --new <csv_candidato> --pipeline otc
python -m pytest tests/
```

---

## SMOKE TESTS INDIVIDUALES

```
python ingest.py --file "data/raw/shares.xlsx" --config config.yaml
python process.py --file "data/raw/shares.xlsx" --fx "data/raw/fx.xlsx"
python update_master.py --file "data/raw/shares.xlsx" --fx "data/raw/fx.xlsx" --dry-run
```

`update_master.py --dry-run` no escribe el maestro.

---

## AGREGAR UN NUEVO LAYOUT

Si el archivo mensual tiene una nueva variante de columnas:

1. Documentar el fingerprint nuevo en `config.yaml → shares_file → columns`
2. Verificar con smoke test: `python ingest.py --file nuevo.xlsx`
3. Correr pipeline completo con `--dry-run` primero
4. Confirmar output y correr sin `--dry-run`

---

## LOGS

OTC escribe `logs/pipeline_YYYYMMDD_HHMMSS.log` y PC/Bev `logs/pipeline_pcbev_YYYYMMDD_HHMMSS.log`.
El nombre tiene precisión de segundos: dos corridas en el mismo segundo comparten archivo.

---

## BACKUPS

Si existe un maestro previo y la corrida es sin `--dry-run` y sin `--no-backup`, se crea un backup antes de escribir:

- OTC: `data/master_backup_YYYYMMDD_HHMMSS.csv`
- PC/Bev: `data/master_pcbev_backup_YYYYMMDD_HHMMSS.csv`
