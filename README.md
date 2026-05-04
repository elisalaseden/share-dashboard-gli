# Share Dashboard · OTC LATAM · Investor Reporting Pipeline
Genomma Lab Internacional — Consumer Insights

---

## ESTRUCTURA DEL PROYECTO

```
share-dashboard/
├── data/
│   ├── raw/              → Excel originales (shares + tipo de cambio)
│   ├── processed/        → archivos normalizados intermedios
│   └── master.csv        → dataset consolidado final (fuente del dashboard)
├── scripts/
│   ├── ingest.py         → lectura + fingerprint de layouts
│   ├── process.py        → pivot + validación + tipo de cambio
│   ├── update_master.py  → escritura idempotente a master.csv
│   └── run_pipeline.py   → orquestador completo (punto de entrada mensual)
├── dashboard/
│   └── app.py            → Streamlit dashboard
├── config.yaml           → fuente de verdad del pipeline
├── requirements.txt
└── README.md
```

---

## INSTALACIÓN (una sola vez)

```bash
# 1. Crear entorno virtual
python -m venv venv
venv\Scripts\activate          # Windows

# 2. Instalar dependencias
pip install -r requirements.txt
```

---

## USO MENSUAL — PIPELINE COMPLETO

```bash
# Activar entorno
cd share-dashboard
venv\Scripts\activate

# Ejecutar pipeline (desde la raíz del proyecto)
python scripts/run_pipeline.py \
  --shares "data/raw/02_2026_Data_OTC_CloseUP_Moneda_local.xlsx" \
  --fx     "data/raw/2026_03_Tipos_de_Cambio.xlsx"

# Con dry-run (simular sin escribir):
python scripts/run_pipeline.py \
  --shares "data/raw/02_2026_Data_OTC_CloseUP_Moneda_local.xlsx" \
  --fx     "data/raw/2026_03_Tipos_de_Cambio.xlsx" \
  --dry-run

# Solo un país (para pruebas):
python scripts/run_pipeline.py \
  --shares "..." --fx "..." --sheets MEX ARG
```

---

## LEVANTAR EL DASHBOARD

```bash
# Desde la raíz del proyecto
streamlit run dashboard/app.py
```

El dashboard abre en: http://localhost:8501

---

## SMOKE TESTS INDIVIDUALES

```bash
# Test de ingest (solo lectura)
python scripts/ingest.py --file "data/raw/shares.xlsx" --config config.yaml

# Test de process (ingest + normalización)
python scripts/process.py \
  --file "data/raw/shares.xlsx" \
  --fx   "data/raw/fx.xlsx"

# Test de update_master (pipeline completo, sin escribir)
python scripts/update_master.py \
  --file "data/raw/shares.xlsx" \
  --fx   "data/raw/fx.xlsx" \
  --dry-run
```

---

## AGREGAR UN NUEVO LAYOUT

Si el archivo mensual tiene una nueva variante de columnas:

1. Documentar el fingerprint nuevo en `config.yaml → shares_file → columns`
2. Verificar con smoke test: `python scripts/ingest.py --file nuevo.xlsx`
3. Correr pipeline completo con `--dry-run` primero
4. Confirmar output y correr sin `--dry-run`

---

## LOGS

Los logs se generan en `logs/pipeline_YYYYMMDD_HHMMSS.log`.
Cada ejecución genera su propio log. No se sobreescriben.

---

## BACKUPS

Cada ejecución del pipeline crea automáticamente:
`data/master_backup_YYYYMMDD_HHMMSS.csv`

Para desactivar backups: `--no-backup`
