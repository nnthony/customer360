# Customer 360 — Pipeline incremental de pagos

![Tests](https://github.com/nnthony/customer360/actions/workflows/tests.yml/badge.svg)

Pipeline de datos en **Python + DuckDB** que construye una vista **Customer 360** (una fila por cliente) a partir de cuatro fuentes: clientes, servicios, pagos y contactos. Sigue la arquitectura *medallion* (**Raw → Bronze → Silver → Gold**).

El proyecto empezó como un pipeline batch de carga completa y evolucionó a un **pipeline incremental**: los pagos llegan en lotes, se aplican con `MERGE` dentro de una transacción y el resultado es **idempotente**, **auditable** y **recuperable ante fallos**.

> **Punto de entrada principal:** `python -m src.pipeline_incremental`
> El pipeline batch original (`src.pipeline`) se conserva como referencia y sus módulos se reutilizan.

---

## Tabla de contenidos

1. [Inicio rápido](#inicio-rápido)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Arquitectura](#arquitectura)
4. [Pipeline incremental](#pipeline-incremental)
5. [Reglas de negocio (Gold)](#reglas-de-negocio-gold)
6. [Reglas de calidad (Silver)](#reglas-de-calidad-silver)
7. [Estado persistente (DuckDB)](#estado-persistente-duckdb)
8. [Ejemplo con los datos incluidos](#ejemplo-con-los-datos-incluidos)
9. [Pruebas](#pruebas)
10. [Pipeline batch original](#pipeline-batch-original)
11. [Evolución del proyecto](#evolución-del-proyecto)

---

## Inicio rápido

### Requisitos

- Python **3.10+**
- `duckdb`
- `pytest` (solo para las pruebas)

```bash
python -m venv env
# Windows
env\Scripts\activate
# Linux / macOS
source env/bin/activate

pip install duckdb pytest
```

### Ejecutar

Todos los comandos se ejecutan **desde la raíz del proyecto** (`customer360/`).

```bash
python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_inicial.csv data/raw/pagos/pagos_lote_001.csv data/raw/pagos/pagos_lote_002.csv
```

En Linux/macOS (bash) se puede usar un comodín: `--lote data/raw/pagos/*.csv`.
En **PowerShell** el comodín no se expande al pasarlo a Python, así que hay que listar los archivos como en el ejemplo de arriba.

### Opciones de la línea de comandos

| Opción | Obligatoria | Descripción |
|---|---|---|
| `--lote RUTA [RUTA ...]` | Sí | Uno o más CSV de pagos. Se aplican **en orden alfabético**, un lote por ejecución interna. |
| `--fecha-corte YYYY-MM-DD` | Sí | Fecha de referencia para las ventanas de 30 y 90 días de Gold. |
| `--base-dir RUTA` | No | Carpeta con `raw/`, `bronze/`, `silver/`, `gold/`. Por defecto: `data/`. |
| `--simular-fallo` | No | Lanza un error justo **antes del COMMIT** para probar la recuperación. |
| `--debug` | No | Muestra el traceback completo en lugar del mensaje resumido. |

Código de salida: `0` si todos los lotes terminan en `SUCCESS`, `1` en cuanto uno falla (los siguientes no se procesan).

---

## Estructura del proyecto

```
customer360/
├── src/
│   ├── pipeline_incremental.py   # ★ Orquestador incremental (punto de entrada principal)
│   ├── incremental.py            # Lógica de lotes: Bronze append-only, dedup, clasificación, MERGE
│   ├── ingest.py                 # Raw -> Bronze (copia exacta + manifiesto con SHA-256)
│   ├── transform.py              # Bronze -> Silver (reglas de calidad, dedup, rechazados)
│   └── pipeline.py               # Pipeline batch original + contrato de Gold (reutilizado)
├── sql/
│   ├── schema.sql                # Tablas del estado persistente (idempotente)
│   ├── clasificar_pagos.sql      # INSERT / UPDATE / IGNORE / CONFLICT por pago
│   ├── merge_pagos.sql           # MERGE del lote sobre silver_pagos
│   └── customer360.sql           # Construcción de Gold (mismo SQL para batch e incremental)
├── tests/
│   ├── fixtures/pagos_lote_conflicto.csv
│   ├── test_aceptacion.py        # 8 casos de aceptación del pipeline incremental
│   ├── test_pipeline_incremental.py
│   ├── test_merge.py
│   ├── test_clasificacion.py
│   ├── test_lote_pagos.py
│   ├── test_gold.py
│   ├── test_transform.py
│   ├── test_ingest.py
│   └── test_pipeline.py
└── data/
    ├── raw/
    │   ├── clientes.csv          # Carga completa
    │   ├── servicios.csv         # Carga completa
    │   ├── contactos.csv         # Carga completa
    │   ├── pagos.csv             # Solo para el pipeline batch original
    │   └── pagos/                # Lotes incrementales: pagos_<batch_id>.csv
    │       ├── pagos_inicial.csv
    │       ├── pagos_lote_001.csv
    │       └── pagos_lote_002.csv
    ├── bronze/                   # Copias inmutables + _manifest.json
    │   └── pagos/<batch_id>.csv  # Un archivo por lote (append-only)
    ├── silver/                   # Parquet tipados + silver/rechazados/
    ├── gold/                     # customer360.parquet / customer360.csv (exportados)
    ├── logs/                     # Logs JSON del pipeline batch
    └── customer360.duckdb        # ★ Estado oficial del pipeline incremental
```

---

## Arquitectura

| Capa | Qué contiene | Cómo se escribe |
|---|---|---|
| **Raw** | Archivos tal como llegan del sistema origen | No se modifica |
| **Bronze** | Copia byte a byte de Raw + manifiesto (filas, SHA-256, fecha) | Cargas completas: se reemplazan. Lotes de pagos: **append-only**, nunca se sobrescriben |
| **Silver** | Datos tipados y validados; cada fila rechazada queda registrada con su motivo | Cargas completas: reemplazo. Pagos: **MERGE incremental** |
| **Gold** | `customer360`: una fila por cliente con métricas y elegibilidad | Se reconstruye completo desde Silver en cada ejecución |

Dos tipos de fuente:

- **Carga completa** (`clientes`, `servicios`, `contactos`): cada ejecución las vuelve a leer completas desde `data/raw/` y reemplaza su versión en Silver.
- **Incremental** (`pagos`): llegan en lotes con una columna extra `fecha_actualizacion` que permite ordenar versiones de un mismo pago.

**La base DuckDB (`data/customer360.duckdb`) es la fuente oficial.** Los archivos de `data/gold/` son copias derivadas que se regeneran después de cada `COMMIT`.

---

## Pipeline incremental

### Flujo de un lote

```
Fuera de la transacción (archivos, idempotentes)
  1. Bronze + Silver de las cargas completas (clientes, servicios, contactos)
  2. Copia del lote de pagos a bronze/pagos/<batch_id>.csv (append-only)

BEGIN TRANSACTION
  3. Reemplazar silver_clientes / silver_servicios / silver_contactos
  4. Preparar el lote: validar -> tipar -> deduplicar dentro del lote
  5. Clasificar cada pago contra el estado: INSERT / UPDATE / IGNORE / CONFLICT
  6. Registrar el lote en control_lotes (batch_id + SHA-256)
  7. MERGE sobre silver_pagos (+ verificación de post-condiciones)
  8. Reemplazar los rechazados del lote en silver_pagos_rechazados
  9. Reconstruir gold_customer360 y verificar su contrato
 10. Fila SUCCESS en auditoria_ejecuciones
COMMIT          <- Silver, Gold y auditoría se confirman JUNTOS

 11. Exportar gold/customer360.parquet y .csv (escritura atómica con os.replace)

Si algo falla: ROLLBACK y, después (fuera de la transacción), fila FAILED en la auditoría.
```

### Convención de nombres de lote

Los archivos deben llamarse `pagos_<batch_id>.csv` (minúsculas, dígitos y `_`). El `batch_id` se toma del nombre:

| Archivo | `batch_id` |
|---|---|
| `pagos_inicial.csv` | `inicial` |
| `pagos_lote_001.csv` | `lote_001` |
| `pagos_lote_002.csv` | `lote_002` |

Columnas obligatorias, en este orden:
`pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion`

Como los lotes se aplican en orden alfabético, conviene numerarlos con ceros a la izquierda (`lote_001`, `lote_002`, …, `lote_010`).

### Precedencia entre versiones de un pago

Para un mismo `pago_id`:

1. **Gana la de mayor `fecha_actualizacion`.**
2. Misma fecha y mismo contenido → **duplicado**: se conserva una y la otra se cuenta como ignorada.
3. Misma fecha y **distinto** contenido → **conflicto**: el lote completo falla y no se aplica nada.

Nunca se elige una versión al azar. La regla se aplica dos veces: dentro del lote y contra la versión vigente en `silver_pagos`.

### Clasificación contra el estado

| Situación | `detalle` | Acción |
|---|---|---|
| El pago no existe en el estado | `nuevo` | `INSERT` |
| `fecha_actualizacion` del lote > la vigente | `version_posterior` | `UPDATE` |
| `fecha_actualizacion` del lote < la vigente | `version_antigua` | `IGNORE` |
| Misma fecha y mismo contenido de negocio | `identico` | `IGNORE` |
| Misma fecha y distinto contenido | `conflicto` | `CONFLICT` → el lote falla |

La columna `batch_id` es linaje (metadato), **no** dato de negocio: nunca se usa para decidir si un pago cambió. Así, reaplicar un lote no genera actualizaciones falsas.

### Garantías

- **Idempotencia:** reaplicar el mismo lote deja `silver_pagos`, rechazados, `control_lotes` y Gold **idénticos**. Solo se agrega una fila nueva en la auditoría.
- **Atomicidad:** Silver, Gold y la auditoría SUCCESS se confirman en una sola transacción. Un fallo deja el estado tal como estaba tras el último `COMMIT`.
- **Evidencia de fallos:** la fila FAILED se escribe *después* del `ROLLBACK`, para que el rollback no la borre.
- **Protección del `batch_id`:** reutilizar un `batch_id` con otro contenido falla en dos lugares: en Bronze (comparando hashes) y en `control_lotes` (aunque se haya borrado `data/bronze/`).
- **Auditoría que cuadra:** en cada lote se verifica
  `entrada = insertados + actualizados + ignorados + rechazados`.
- **Exportación segura:** los archivos de Gold se escriben con nombre temporal y luego se renombran, así que un lector nunca ve un archivo a medio escribir. Si la exportación falla, la base sigue correcta y basta con volver a ejecutar.

### Salida en consola

```
[lote_001 ] SUCCESS  entrada=  8  ins=  2  upd=  2  ign=  3  rech=  1  |  gold 10 filas, elegibles=5
```

| Campo | Significado |
|---|---|
| `entrada` | Filas leídas del CSV del lote |
| `ins` | Pagos nuevos insertados en `silver_pagos` |
| `upd` | Pagos existentes reemplazados por una versión posterior |
| `ign` | Duplicados y versiones viejas dentro del lote, más versiones antiguas o idénticas respecto al estado |
| `rech` | Filas que no pasaron las reglas de calidad (quedan en `silver_pagos_rechazados`) |
| `gold` | Filas de `customer360` y cuántos clientes son elegibles |

---

## Reglas de negocio (Gold)

`sql/customer360.sql` produce **una fila por cliente de Silver**, aunque no tenga servicios, pagos ni contactos (en ese caso las métricas valen 0).

| Columna | Definición |
|---|---|
| `cliente_id` | Identificador del cliente |
| `cantidad_servicios_activos` | Servicios con `estado = 'ACTIVO'` |
| `cargo_mensual_total` | Suma de `cargo_mensual` de los servicios **activos** |
| `ultimo_pago_exitoso` | Fecha máxima de pago `EXITOSO` hasta la fecha de corte (puede ser `NULL`) |
| `monto_pagado_90d` | Suma de pagos `EXITOSO` en los últimos 90 días |
| `contactos_30d` | Contactos en los últimos 30 días |
| `es_elegible` | `estado = 'ACTIVO'` **y** sin servicios `VENCIDO` **y** `contactos_30d < 2` |

**Convención de ventanas:** "últimos N días" = `fecha > fecha_corte - N AND fecha <= fecha_corte`. Son N días exactos contando el día de corte, y nada posterior al corte entra en el cálculo.

**Contrato de Gold** (si falla, no se publica nada): columnas exactas en orden, una fila por cliente, mismo número de filas que clientes en Silver, y sin `NULL` en columnas obligatorias (salvo `ultimo_pago_exitoso`).

---

## Reglas de calidad (Silver)

Cada fila recibe como mucho **un** motivo de rechazo: el primero que falle, en este orden. Las filas rechazadas nunca se borran en silencio; se guardan con su valor original y su motivo.

| Tabla | Reglas (en orden) |
|---|---|
| `clientes` | `id_invalido` (`^C\d{3}$`) → `fecha_invalida` → `estado_invalido` (`ACTIVO`, `INACTIVO`) |
| `servicios` | `id_invalido` (`^S\d{3}$`) → `cliente_inexistente` → `tipo_invalido` → `estado_invalido` (`ACTIVO`, `VENCIDO`, `CANCELADO`) → `cargo_invalido` (> 0) |
| `contactos` | `id_invalido` (`^T\d{3}$`) → `cliente_inexistente` → `fecha_invalida` → `canal_invalido` (`EMAIL`, `LLAMADA`, `CHAT`) |
| `pagos` (lotes) | `id_invalido` (`^P\d{3}$`) → `cliente_inexistente` → `fecha_invalida` → `monto_invalido` (> 0) → `estado_invalido` (`EXITOSO`, `FALLIDO`) → `fecha_actualizacion_invalida` |

- Antes de validar, los valores se normalizan con `TRIM`, y los campos categóricos además con `UPPER`.
- La integridad referencial (`cliente_inexistente`) se valida contra los clientes **ya limpios** de Silver.
- En las cargas completas, si un id se repite, gana la fila válida y el resto se marca `duplicado`.

---

## Estado persistente (DuckDB)

Archivo: `data/customer360.duckdb`. El esquema (`sql/schema.sql`) se aplica en cada ejecución y es idempotente.

| Tabla | Contenido |
|---|---|
| `silver_pagos` | Una fila por `pago_id` con la **versión vigente** y el `batch_id` que la escribió |
| `silver_pagos_rechazados` | Rechazados por lote (se reemplazan por `batch_id` al reaplicar) |
| `control_lotes` | `batch_id`, archivo, SHA-256 y fecha de registro de cada lote aplicado |
| `auditoria_ejecuciones` | Una fila por **ejecución** (`SUCCESS` / `FAILED`) con conteos y mensaje de error. Es historia: se acumula |
| `silver_clientes`, `silver_servicios`, `silver_contactos` | Cargas completas, recreadas en cada ejecución |
| `gold_customer360` | Vista Customer 360 vigente |

Consultas útiles:

```python
import duckdb
con = duckdb.connect("data/customer360.duckdb", read_only=True)

con.sql("""
    SELECT batch_id, estado, registros_entrada, registros_insertados,
           registros_actualizados, registros_ignorados, registros_rechazados, mensaje_error
    FROM auditoria_ejecuciones ORDER BY fecha_inicio
""").show()

con.sql("SELECT * FROM silver_pagos_rechazados").show()
con.sql("SELECT * FROM gold_customer360 ORDER BY cliente_id").show()
```

### Empezar desde cero

Para reconstruir el estado desde el principio, borra la base y la copia Bronze de los lotes:

```bash
# Windows (PowerShell)
Remove-Item data\customer360.duckdb
Remove-Item -Recurse data\bronze\pagos

# Linux / macOS
rm data/customer360.duckdb
rm -rf data/bronze/pagos
```

Si cambias el contenido de un lote ya aplicado, ponle un **nuevo** `batch_id`. Reutilizar el nombre con otro contenido se rechaza a propósito.

---

## Ejemplo con los datos incluidos

### Qué contiene cada lote

| Lote | Filas | Casos que ejercita |
|---|---|---|
| `pagos_inicial.csv` | 30 | Carga inicial: los 30 pagos históricos (`P001`–`P030`) |
| `pagos_lote_001.csv` | 8 | Pago nuevo (`P031`), duplicado exacto (`P032` ×2), dos versiones de `P003` en el mismo lote (gana la de las 10:00), corrección de `P024` (`FALLIDO` → `EXITOSO`), reenvío idéntico de `P001`, y cliente inexistente (`P033` → `C999`, rechazado) |
| `pagos_lote_002.csv` | 4 | Nueva corrección de `P003`, versión **antigua** de `P024` (se ignora), pago nuevo (`P034`), reenvío idéntico de `P031` |

### Primera ejecución (estado vacío)

```
[inicial  ] SUCCESS  entrada= 30  ins= 30  upd=  0  ign=  0  rech=  0  |  gold 10 filas, elegibles=5
[lote_001 ] SUCCESS  entrada=  8  ins=  2  upd=  2  ign=  3  rech=  1  |  gold 10 filas, elegibles=5
[lote_002 ] SUCCESS  entrada=  4  ins=  1  upd=  1  ign=  2  rech=  0  |  gold 10 filas, elegibles=5
```

Estos son los conteos que verifica `test_auditoria_registra_cada_ejecucion`.

### Reejecución (mismos lotes sobre un estado ya cargado)

```
[inicial  ] SUCCESS  entrada= 30  ins=  0  upd=  0  ign= 30  rech=  0  |  gold 10 filas, elegibles=5
[lote_001 ] SUCCESS  entrada=  8  ins=  0  upd=  0  ign=  7  rech=  1  |  gold 10 filas, elegibles=5
[lote_002 ] SUCCESS  entrada=  4  ins=  0  upd=  0  ign=  4  rech=  0  |  gold 10 filas, elegibles=5
```

Esto demuestra la **idempotencia**: no se inserta ni se actualiza nada, Gold queda idéntico y cada ejecución deja su fila en la auditoría. El rechazado de `lote_001` se vuelve a contar, pero se *reemplaza* en `silver_pagos_rechazados`, no se duplica.

### Gold resultante (`--fecha-corte 2026-09-30`)

| cliente_id | serv. activos | cargo mensual | último pago exitoso | pagado 90d | contactos 30d | elegible |
|---|---:|---:|---|---:|---:|:---:|
| C001 | 0 | 0.00 | 2026-09-29 | 100.00 | 0 | ✅ |
| C002 | 3 | 200.00 | 2026-09-10 | 270.00 | 1 | ✅ |
| C003 | 1 | 90.00 | 2026-09-15 | 90.00 | 1 | ❌ servicio vencido |
| C004 | 1 | 120.00 | 2026-09-28 | 120.00 | 0 | ✅ |
| C005 | 1 | 80.00 | 2026-09-20 | 160.00 | 2 | ❌ 2 contactos |
| C006 | 2 | 145.00 | 2026-09-30 | 435.00 | 0 | ✅ |
| C007 | 1 | 70.00 | 2026-09-05 | 140.00 | 1 | ✅ |
| C008 | 2 | 155.00 | 2026-09-30 | 310.00 | 2 | ❌ 2 contactos |
| C009 | 1 | 90.00 | 2026-09-12 | 180.00 | 1 | ❌ cliente inactivo |
| C010 | 4 | 245.00 | 2026-09-25 | 1080.00 | 4 | ❌ 4 contactos |

Cambios respecto a la carga inicial: `C001` (+`P032`), `C002` (`P003` corregido de 200 a 170), `C004` (primer pago, `P031`), `C006` (+`P034`) y `C010` (`P024` pasa a `EXITOSO`, +100).

### Probar la recuperación ante fallos

```bash
python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_lote_001.csv --simular-fallo
```

El error se lanza con todo ya escrito pero sin confirmar. El resultado esperado es `FAILED` y código de salida `1`. `silver_pagos`, `control_lotes` y Gold quedan como estaban, y en `auditoria_ejecuciones` aparece una fila `FAILED` con `FalloSimuladoError` en `mensaje_error`.

---

## Pruebas

```bash
python -m pytest                 # toda la suite
python -m pytest -m aceptacion -v  # solo los 8 casos de aceptación
```

Las pruebas usan directorios temporales (`tmp_path`), así que no tocan `data/`.

| Archivo | Qué cubre |
|---|---|
| `test_aceptacion.py` | 8 casos: inserción, corrección, versión antigua, reejecución, duplicado, conflicto, fallo parcial y Gold actualizado |
| `test_pipeline_incremental.py` | Secuencia completa, auditoría, reejecución, fallo simulado, conflicto auditado, exportación y CLI |
| `test_merge.py` | MERGE, linaje, independencia del orden de los lotes, rollback y reintento |
| `test_clasificacion.py` | INSERT / UPDATE / IGNORE / CONFLICT contra el estado |
| `test_lote_pagos.py` | `batch_id`, Bronze append-only, dedup y conflictos dentro del lote |
| `test_gold.py` | Ventanas de 30 y 90 días, fan-out, elegibilidad y *golden test* con datos reales |
| `test_transform.py` | Reglas de calidad, tipos, normalización, dedup y reconciliación |
| `test_ingest.py` | Contrato de entrada, copia byte a byte y manifiesto SHA-256 |
| `test_pipeline.py` | Pipeline batch, logs, reconciliación entre capas y CLI |

> El marcador `aceptacion` no está registrado en un `pytest.ini`, así que pytest puede mostrar un `PytestUnknownMarkWarning`. Para quitarlo, crea un `pytest.ini` con:
> ```ini
> [pytest]
> markers =
>     aceptacion: pruebas de aceptación del pipeline incremental
> ```

---

## Pipeline batch original

```bash
python -m src.pipeline --fecha-corte 2026-09-30
```

Procesa las cuatro tablas como carga completa desde `data/raw/` (incluido `data/raw/pagos.csv`, sin `fecha_actualizacion`). Reconcilia los conteos entre capas y guarda un log JSON por ejecución en `data/logs/`. No usa la base DuckDB.

> ⚠️ Ambos pipelines escriben `data/gold/customer360.{parquet,csv}`. Si ejecutas el batch después del incremental, esos archivos reflejarán el cálculo batch (sin los lotes). Para volver al estado oficial, reejecuta el pipeline incremental: es idempotente.

---

## Evolución del proyecto

1. **Bronze (`ingest.py`):** copia exacta de Raw con validación de columnas y manifiesto SHA-256.
2. **Silver (`transform.py`):** reglas de calidad, tipado, deduplicación y tabla de rechazados con reconciliación.
3. **Gold + orquestación batch (`pipeline.py`, `customer360.sql`):** Customer 360 parametrizado por fecha de corte, contrato de Gold (*write-audit-publish*) y logs por ejecución.
4. **Incremental (`incremental.py`, `pipeline_incremental.py`):** lotes de pagos con `fecha_actualizacion`, Bronze append-only, clasificación y `MERGE` transaccional, control de lotes, auditoría e idempotencia. El SQL de Gold es **el mismo** que en la etapa 3: solo cambia de dónde lee (tablas de DuckDB en vez de Parquet).