---
name: pipeline-dry
description: Corre un pipeline del share dashboard en modo --dry-run y reporta válidos, rechazados, nuevos y reemplazados antes de escribir el maestro.
disable-model-invocation: true
argument-hint: otc|pcbev <archivo(s) de shares> [--fx <tipo de cambio>]
---

# /pipeline-dry

Simula una corrida mensual sin escribir el maestro. La invoca Eli; el modelo no la dispara solo.

El dry-run simula el upsert y devuelve estadísticas. No genera un CSV candidato ni acredita una reconstrucción completa, así que no hay nada que pasarle a `master-validator`.

## Pasos

1. Identifica el pipeline (`otc` o `pcbev`) y los archivos en `$ARGUMENTS`. Si falta el de tipo de cambio o su selección es ambigua, detente y pregunta. No lo elijas tú.
2. Antes de correr, registra: `git status --short`, filas del maestro actual y SHA-256 de los maestros protegidos (`master.csv`, `data/master*.csv`, incluidos backups e ignorados) y de los Excel de entrada.
3. Ejecuta, siempre con `--dry-run`:
   - OTC: `python run_pipeline.py --shares "<archivo>" --fx "<fx>" --dry-run`
   - PC/Bev: `python run_pipeline_pcbev.py --files "<archivo>" ... --fx "<fx>" --dry-run`
4. Si el comando falla, muestra el error literal y detente. No modifiques `config.yaml` ni el código para que pase.
5. Lee el resumen de la consola y el log nuevo de `logs/` (`pipeline_...log` u `pipeline_pcbev_...log`). Reporta en una tabla:
   - filas raw, válidos, rechazados (total y por motivo), nuevos, reemplazados y total final esperado;
   - los primeros 10 rechazos literales y la ruta del log.
6. Repite los SHA-256 del paso 2 y comprueba que no cambiaron. Si alguno difiere, reporta el incidente y detente, sin restaurar ni corregir nada. `git status` es información complementaria. Identifica el log que creó el dry-run.
7. Termina con la decisión pendiente para Eli: correr sin `--dry-run` o no. Nunca la ejecutes desde esta skill.

## Límites

- El reporte describe la corrida. Los maestros no son fuente autorizada para citar shares.
- Si `brand_consolidation` cambió desde la última corrida, avisa que el upsert conserva las variantes viejas y que la reconstrucción está pendiente (CLAUDE.md, regla 3).
