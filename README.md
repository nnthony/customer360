# Customer 360 — Pipeline local Bronze → Silver → Gold

El presente proyecto se desarrollo para la generación de una audiencia a partir de datos estructurados procesados y filtrados por reglas de negocio predefinidas. Se utilizó una estructura medallon de ingeniería de datos utlizando python + duckdb

## Arquitectura

```
data/raw/      ──ingest.py──▶  data/bronze/   ──transform.py──▶  data/silver/   ──customer360.sql──▶  data/gold/
```

| Capa | Contenido | Formato | Regla principal |
|---|---|---|---|
| Raw |Fuentes originales |.csv | ninguna |
| Bronze | Fuente a usar | .csv | Fuentes registradas |
| Silver | Data limpia  | .parquet | Exclusion de valores nulos e inconsistentes |
| Gold | Audiencia seleccionada | .parquet y .csv | Audiencia general filtrada por reglas de negocio |


## Requisitos e instalación

```powershell
duckdb==1.5.6
pytest==9.1.1
```


## Ejecución


```powershell
python -m src.pipeline --fecha-corte 2026-09-30
```

```
[bronze] clientes     10 filas
[bronze] servicios    20 filas
[bronze] pagos        30 filas
[bronze] contactos    20 filas
[silver] clientes   validos=  10  rechazados=  0  {}
[silver] servicios  validos=  20  rechazados=  0  {}
[silver] pagos      validos=  30  rechazados=  0  {}
[silver] contactos  validos=  20  rechazados=  0  {}
[gold]   customer360  10 filas  elegibles=5  (corte 2026-09-30)
[ok]     0.618 s  ->  C:\Users\HP\anaconda3\codigos\DataE\customer360\data\logs\run_20261018T015837452302.json
```


## Estructura del proyecto

```
customer360/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── ingest.py - script de ingesta de datos 
│   ├── transform.py - script de funciones de transformacion
│   └── pipeline.py - script orquestador y generador de logs
├── sql/
│   └── customer360.sql - query de generacion de filtros de audiencia
├── tests/ 
│   └── test_gold.py - script pytest para testeo de audiencia final
│   └── test_ingest.py - script pytest para testeo de funciones de ingesta
│   └── test_pipeline.py - script pytest para testeo de funciones de pipeline
│   └── test_transform.py - script pytest para testeo de funciones de transformacion
├── requirements.txt
└── README.md
```


## Pruebas

```powershell
python -m pytest -v
=========================================================== 84 passed in 16.61s ===========================================================
```
