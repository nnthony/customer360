# Customer 360 — Pipeline de datos local, incremental y verificable

![Tests](https://github.com/nnthony/customer360/actions/workflows/tests.yml/badge.svg)

Pipeline local construido con **Python + DuckDB + Parquet + pytest** sobre la
arquitectura medallón (Bronze → Silver → Gold). A partir de cuatro fuentes
(clientes, servicios, pagos y contactos) genera una tabla **Customer 360** con
una fila por cliente para seleccionar una audiencia objetivo.

Los **pagos se procesan de forma incremental**: cada lote puede traer pagos
nuevos, correcciones y registros repetidos. El pipeline garantiza que
reejecutar un lote no cambia el resultado, que una versión antigua nunca pisa
a una corrección y que un fallo a mitad del proceso no corrompe el estado.

Cada `push` y cada pull request ejecuta las 133 pruebas en GitHub Actions.

---

## Índice

1. [Inicio rápido](#inicio-rápido)
2. [Arquitectura](#arquitectura)
3. [Estructura del proyecto](#estructura-del-proyecto)
4. [Ejecución](#ejecución)
5. [Capas y reglas de calidad](#capas-y-reglas-de-calidad)
6. [Customer 360 (Gold)](#customer-360-gold)
7. [Carga incremental de pagos](#carga-incremental-de-pagos)
8. [Transacción, recuperación y auditoría](#transacción-recuperación-y-auditoría)
9. [Pruebas](#pruebas)
10. [Integración continua](#integración-continua)
11. [Docker](#docker)
12. [Qué se versiona y qué no](#qué-se-versiona-y-qué-no)
13. [Decisiones de diseño](#decisiones-de-diseño)
14. [Respuesta a observaciones anteriores](#respuesta-a-observaciones-anteriores)
15. [Limitaciones y siguientes pasos](#limitaciones-y-siguientes-pasos)

---

## Inicio rápido

**Requisitos:** Python **3.10** y Git. No se necesita ninguna otra configuración.

**Windows (PowerShell):**

```powershell
git clone https://github.com/nnthony/customer360.git
cd customer360
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -ra
```

**Linux / macOS:**

```bash
git clone https://github.com/nnthony/customer360.git
cd customer360
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -ra
```

Resultado esperado: **`133 passed`**, sin ninguna línea `SKIPPED`.

> Se verificó clonando el repositorio en una carpeta vacía, con un entorno
> virtual nuevo, en Windows (Python 3.10.11) y en Ubuntu vía GitHub Actions
> (Python 3.10.22).

### Dependencias

`requirements.txt` contiene **solo las dependencias directas**, con versión fija:

```
duckdb==1.5.6
pytest==9.1.1
```

No se usa `pip freeze`: arrastraría dependencias propias del sistema operativo.
Por ejemplo, pytest instala `colorama` solo en Windows; congelarla haría que
el archivo describa una máquina concreta y no el proyecto.

---

## Arquitectura

```
data/raw/                     (entrada, versionada en Git)
 ├── clientes.csv ─┐
 ├── servicios.csv ┼── ingest.py ──▶ bronze/*.csv ──transform.py──▶ silver/*.parquet ─┐
 ├── contactos.csv ┘                (copia exacta + manifiesto sha256)  (validado)      │
 └── pagos/                                                                              │
     ├── pagos_inicial.csv ──┐                                                           │
     ├── pagos_lote_001.csv ─┼── incremental.py ──▶ bronze/pagos/<batch_id>.csv          │
     └── pagos_lote_002.csv ─┘                     (append-only, nunca se sobrescribe)  │
                                                                                         ▼
                 ┌──────────────── data/customer360.duckdb (ESTADO OFICIAL) ────────────────┐
   BEGIN ───────▶│ silver_clientes · silver_servicios · silver_contactos  (carga completa)  │
                 │ silver_pagos (MERGE incremental) · silver_pagos_rechazados               │
                 │ control_lotes · gold_customer360 · auditoria_ejecuciones (SUCCESS)       │
   COMMIT ──────▶└─────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼  (después del COMMIT)
                              data/gold/customer360.parquet / .csv   (copias derivadas)

   Si algo falla: ROLLBACK → auditoría FAILED registrada fuera de la transacción.
```

**Principios:**

- **Bronze** conserva la entrada tal cual llegó, sin interpretar.
- **Silver** valida, tipa y deduplica según reglas documentadas. Ninguna fila
  se borra en silencio: termina como válida o como rechazada, con su motivo.
- **Gold** aplica la lógica de negocio y se reconstruye completo desde Silver.
- La **base DuckDB es la fuente oficial**. Los Parquet/CSV de `data/gold/` son
  copias que se pueden regenerar en cualquier momento.

---

## Estructura del proyecto

```
customer360/
├── .github/workflows/tests.yml   # CI: pytest en cada push y pull request
├── Dockerfile                    # imagen que ejecuta las pruebas
├── .dockerignore
├── data/
│   └── raw/                      # ENTRADA (versionada)
│       ├── clientes.csv, servicios.csv, contactos.csv, pagos.csv
│       └── pagos/                # lotes incrementales
│           ├── pagos_inicial.csv
│           ├── pagos_lote_001.csv
│           └── pagos_lote_002.csv
├── sql/
│   ├── customer360.sql           # lógica de Gold (no contiene rutas)
│   ├── schema.sql                # tablas del estado persistente
│   ├── clasificar_pagos.sql      # decide INSERT / UPDATE / IGNORE / CONFLICT
│   └── merge_pagos.sql           # ejecuta lo decidido
├── src/
│   ├── ingest.py                 # Raw → Bronze
│   ├── transform.py              # Bronze → Silver (reglas de calidad)
│   ├── incremental.py            # lotes de pagos: Bronze, preparación, clasificación, MERGE
│   ├── pipeline_incremental.py   # ★ PUNTO DE ENTRADA OFICIAL
│   └── pipeline.py               # pipeline de carga completa (Semana 3, referencia)
├── tests/
│   ├── fixtures/pagos_lote_conflicto.csv
│   ├── test_aceptacion.py        # las 8 pruebas de aceptación
│   ├── test_calidad.py           # prueba de reconciliación integral
│   └── test_*.py                 # pruebas de desarrollo por capa
├── pytest.ini
└── requirements.txt
```

`pipeline.py` es el orquestador de carga completa de la Semana 3. Se conserva
como referencia y sigue cubierto por sus pruebas, pero **el punto de entrada
oficial es `pipeline_incremental.py`**.

---

## Ejecución

### Procesar la carga inicial y los lotes

```powershell
python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_inicial.csv data/raw/pagos/pagos_lote_001.csv data/raw/pagos/pagos_lote_002.csv
```

Los lotes se aplican en orden alfabético aunque se pasen desordenados. Salida:

```
[inicial  ] SUCCESS  entrada= 30  ins= 30  upd=  0  ign=  0  rech=  0  |  gold 10 filas, elegibles=5
[lote_001 ] SUCCESS  entrada=  8  ins=  2  upd=  2  ign=  3  rech=  1  |  gold 10 filas, elegibles=5
[lote_002 ] SUCCESS  entrada=  4  ins=  1  upd=  1  ign=  2  rech=  0  |  gold 10 filas, elegibles=5
```

### Reejecutar un lote (idempotencia)

```powershell
python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_lote_002.csv
```

```
[lote_002 ] SUCCESS  entrada=  4  ins=  0  upd=  0  ign=  4  rech=  0  |  gold 10 filas, elegibles=5
```

### Simular un fallo antes del COMMIT

```powershell
python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_lote_002.csv --simular-fallo
```

El proceso termina con código de salida `1`, el estado queda intacto y la
auditoría registra la ejecución como `FAILED`.

### Opciones

| Opción | Descripción |
|---|---|
| `--fecha-corte YYYY-MM-DD` | **Obligatoria.** Referencia para las ventanas de 30 y 90 días |
| `--lote <csv> [<csv> ...]` | Uno o más lotes de pagos |
| `--base-dir <carpeta>` | Carpeta de datos (por defecto `data/`) |
| `--simular-fallo` | Lanza un error justo antes del COMMIT |
| `--debug` | Muestra el traceback completo en caso de error |

### Consultar el estado

```powershell
python -c "import duckdb; duckdb.connect('data/customer360.duckdb', read_only=True).sql('SELECT * FROM auditoria_ejecuciones ORDER BY fecha_inicio').show()"
python -c "import duckdb; duckdb.connect('data/customer360.duckdb', read_only=True).sql('SELECT * FROM gold_customer360').show()"
```

---

## Capas y reglas de calidad

### Bronze

- Copia **byte a byte** de la entrada. Todo se lee como texto (`all_varchar`):
  un valor raro no debe romper la lectura.
- **Contrato de entrada:** se valida que existan los archivos y que sus columnas
  sean exactamente las esperadas, **antes** de escribir nada.
- Las tablas de carga completa generan un `_manifest.json` con filas, fecha de
  ingesta y **sha256** por archivo.
- Los lotes de pagos son **append-only**: `bronze/pagos/<batch_id>.csv` nunca se
  sobrescribe. Reaplicar el mismo lote está permitido; reutilizar un `batch_id`
  con otro contenido es un error.

### Silver

Cada fila recibe un `motivo_rechazo` (el **primero** que falle, en el orden de
la tabla). Las válidas se publican tipadas; las rechazadas se guardan **con su
valor original en texto**, para poder diagnosticar qué llegó mal.

| Tabla | Regla | Motivo |
|---|---|---|
| Todas | ID nulo o fuera de formato (`C###`, `S###`, `P###`, `T###`) | `id_invalido` |
| Servicios, pagos, contactos | `cliente_id` no existe entre los clientes **válidos** | `cliente_inexistente` |
| Clientes, pagos, contactos | Fecha no convertible a `DATE` | `fecha_invalida` |
| Servicios | Tipo de servicio nulo | `tipo_invalido` |
| Servicios / pagos | Cargo o monto no numérico o `<= 0` | `cargo_invalido` / `monto_invalido` |
| Clientes | Estado fuera de `ACTIVO`, `INACTIVO` | `estado_invalido` |
| Servicios | Estado fuera de `ACTIVO`, `VENCIDO`, `CANCELADO` | `estado_invalido` |
| Pagos | Estado fuera de `EXITOSO`, `FALLIDO` | `estado_invalido` |
| Contactos | Canal fuera de `EMAIL`, `LLAMADA`, `CHAT` | `canal_invalido` |
| Pagos (lotes) | `fecha_actualizacion` no convertible a `TIMESTAMP` | `fecha_actualizacion_invalida` |
| Carga completa | ID repetido | `duplicado` |

Las reglas de catálogo usan `x IS NULL OR x NOT IN (...)`: `NULL NOT IN (...)`
devuelve `NULL` y no `TRUE`, así que sin el `IS NULL` un valor vacío pasaría.

**Reconciliación:** en cada tabla, `entrada = válidos + rechazados`, y entre
capas, lo que Silver leyó debe ser lo que Bronze guardó. Si no cuadra, el
pipeline falla.

---

## Customer 360 (Gold)

Una fila por cliente, incluidos los que no tienen servicios, pagos ni contactos.
Cada tabla se **agrega antes de unirse** para evitar el *fan-out* (un `JOIN`
directo multiplicaría montos por cantidad de contactos).

| Columna | Definición |
|---|---|
| `cliente_id` | Identificador del cliente |
| `cantidad_servicios_activos` | Servicios con estado `ACTIVO` |
| `cargo_mensual_total` | Suma del cargo de los servicios `ACTIVO` (0 si no tiene) |
| `ultimo_pago_exitoso` | Fecha del último pago `EXITOSO` con `fecha_pago <= corte` (`NULL` si no tiene) |
| `monto_pagado_90d` | Suma de pagos `EXITOSO` con `corte - 90 < fecha_pago <= corte` |
| `contactos_30d` | Contactos con `corte - 30 < fecha_contacto <= corte` |
| `es_elegible` | Cliente `ACTIVO`, **sin** servicios `VENCIDO` y con **menos de 2** contactos en 30 días |

**Convención de ventanas:** "últimos N días" son N días exactos contando el día
de corte, y nada posterior al corte.

**Contrato de Gold** (si falla, no se publica): columnas exactas y en orden, una
fila por cliente, tantas filas como clientes en Silver y sin `NULL` en columnas
obligatorias.

Con corte `2026-09-30` y los tres lotes aplicados: **10 clientes, 5 elegibles**
(C001, C002, C004, C006, C007).

---

## Carga incremental de pagos

Silver de pagos es **incremental**; clientes, servicios y contactos siguen
siendo carga completa; **Gold se reconstruye completo** desde Silver en cada
ejecución. Es una simplificación deliberada: evita introducir incrementalidad en
dos capas a la vez.

Cada pago trae `fecha_actualizacion`, que decide qué versión prevalece.

### Flujo de un lote

```
lote CSV ─▶ validar (reglas de Silver) ─▶ deduplicar DENTRO del lote ─▶ clasificar CONTRA el estado ─▶ MERGE
             lote_validado                  lote_staging                   lote_clasificado
```

### Paso 1: deduplicación dentro del lote

| Situación | Resultado |
|---|---|
| Filas idénticas (incluida la fecha) | Se conserva una (`DISTINCT`) |
| Mismo `pago_id`, distinta `fecha_actualizacion` | Gana la más reciente |
| Mismo `pago_id` y fecha, contenido distinto | **Conflicto: el lote completo falla** |

El conflicto se busca **después** del `DISTINCT` (si no, un duplicado inocente
parecería un conflicto). Y como el chequeo de conflictos garantiza que no hay
empates, el `ROW_NUMBER()` que elige la versión más reciente es determinístico.

### Paso 2: clasificación contra el estado

| Situación | `detalle` | Acción |
|---|---|---|
| El `pago_id` no existe | `nuevo` | `INSERT` |
| `fecha_actualizacion` posterior | `version_posterior` | `UPDATE` |
| `fecha_actualizacion` anterior | `version_antigua` | `IGNORE` |
| Misma fecha y mismos valores | `identico` | `IGNORE` |
| Misma fecha y algún valor distinto | `conflicto` | **El lote falla** |

"Mismos valores" compara las **columnas de negocio** (`cliente_id`,
`fecha_pago`, `monto`, `estado`) con `IS NOT DISTINCT FROM`. **`batch_id` no se
compara**: describe qué lote trajo el dato, no el dato. Si se comparara, un
reenvío idéntico desde otro lote se vería como un conflicto.

### Paso 3: MERGE

El `MERGE` **no decide nada**: su `USING` filtra `accion IN ('INSERT', 'UPDATE')`
y ejecuta lo que decidió la clasificación. Después verifica dos
post-condiciones: que el estado creció exactamente en los `INSERT` decididos y
que cada `INSERT`/`UPDATE` quedó escrito con la versión del lote.

### Lotes de ejemplo

Cada fila existe para ejercitar una regla:

| Lote | Fila | Caso | Resultado |
|---|---|---|---|
| `lote_001` | P031, P032 | Pagos nuevos | INSERT |
| | P032 repetido | Duplicado dentro del lote | IGNORE |
| | P003 a las 09:00 y a las 10:00 | Dos versiones en el lote | Gana 10:00 |
| | P003: 200 → 180 | Corrección de monto | UPDATE |
| | P024: `FALLIDO` → `EXITOSO` | Corrección de estado (cambia Gold) | UPDATE |
| | P001 idéntico | Reenvío sin cambios | IGNORE |
| | P033 con cliente C999 | Regla de Silver | RECHAZADO |
| `lote_002` | P003: 180 → 170 | Segunda corrección | UPDATE |
| | P024 `FALLIDO` del 25/09 | **Versión antigua que llega tarde** | IGNORE |
| | P034 | Pago nuevo | INSERT |
| | P031 idéntico | Reenvío entre lotes | IGNORE |
| `tests/fixtures/pagos_lote_conflicto.csv` | P031 dos veces, misma fecha, 120 y 110 | Conflicto | **Lote rechazado** |

El lote de conflicto vive en `tests/fixtures/` y no en `data/raw/`: si estuviera
con los lotes reales, el pipeline principal fallaría siempre.

**Conteos:** `entrada = insertados + actualizados + ignorados + rechazados`

| Lote | Entrada | Insertados | Actualizados | Ignorados | Rechazados |
|---|---|---|---|---|---|
| inicial | 30 | 30 | 0 | 0 | 0 |
| lote_001 | 8 | 2 | 2 | 3 | 1 |
| lote_002 | 4 | 1 | 1 | 2 | 0 |

**Efecto en Gold** (corte 2026-09-30):

| Cliente | Antes | Después | Por qué |
|---|---|---|---|
| C001 | 50 | 100 | + P032 |
| C002 | 300 | 270 | P003: 200 → 180 → 170 |
| C004 | 0 | 120 | + P031 |
| C006 | 290 | 435 | + P034 |
| C010 | 980 | 1080 | P024 pasa a `EXITOSO` |

---

## Transacción, recuperación y auditoría

Todo el estado se confirma **junto o no se confirma**:

```
FUERA de la transacción (archivos, idempotentes)
    Bronze y Silver de cargas completas · Bronze append-only del lote
BEGIN
    silver_clientes / servicios / contactos   (CREATE OR REPLACE; el DDL de DuckDB es transaccional)
    preparar + clasificar el lote
    control_lotes + MERGE silver_pagos + rechazados del lote
    reconstruir gold_customer360
    auditoría SUCCESS
COMMIT
exportar data/gold/ (archivo temporal + os.replace)

Si falla:  ROLLBACK → auditoría FAILED (otra transacción) → el error se relanza
```

- **Recuperación:** si cualquier paso falla antes del `COMMIT`, el `ROLLBACK`
  deshace Silver, Gold, `control_lotes` y la fila SUCCESS. El lote se puede
  **reintentar** sin limpiar nada a mano.
- **La evidencia sobrevive:** la fila `FAILED` se escribe **después** del
  `ROLLBACK`. Si se escribiera dentro, el `ROLLBACK` la borraría junto con todo.
- **Exportación:** cada archivo se escribe con nombre temporal y se renombra.
  Un lector nunca ve un archivo a medias, y si la exportación falla, basta con
  reexportar: la base sigue correcta.

### Auditoría (`auditoria_ejecuciones`)

| Campo | Descripción |
|---|---|
| `run_id` | UUID de la ejecución |
| `batch_id` | Lote procesado |
| `fecha_inicio`, `fecha_fin` | Duración de la ejecución |
| `estado` | `SUCCESS` / `FAILED` |
| `registros_entrada` | Filas recibidas |
| `registros_insertados` | Altas aplicadas |
| `registros_actualizados` | Correcciones aplicadas |
| `registros_ignorados` | Repetidos y versiones antiguas |
| `registros_rechazados` | Filas que no pasaron las reglas de Silver |
| `mensaje_error` | Tipo y mensaje del error (`NULL` si fue exitosa) |

En una fila `FAILED`, `registros_entrada` se registra si se llegó a leer el
lote, y los conteos aplicados quedan en **0**: el `ROLLBACK` deshizo todo, así
que reportar cambios sería falso.

La auditoría es **historia**: se acumula y no forma parte del estado que debe
quedar idéntico al reejecutar.

---

## Pruebas

```powershell
python -m pytest -ra               # las 133 pruebas
python -m pytest -m aceptacion -v  # las 8 pruebas de aceptación
python -m pytest -m calidad -v     # la prueba de reconciliación integral
```

`-ra` lista al final cualquier prueba saltada: un *skip* silencioso sería un
falso verde.

### Pruebas de aceptación (`tests/test_aceptacion.py`)

Cada una aplica un lote mínimo (1-2 filas) escrito dentro del propio test, para
que se vea qué se prueba sin abrir otro archivo. Siguen la estructura
**Dado / Cuando / Entonces**.

| # | Prueba | Qué demuestra |
|---|---|---|
| 1 | Inserción | Un pago nuevo aparece exactamente una vez |
| 2 | Corrección | Una versión posterior cambia el monto |
| 3 | Versión antigua | Una versión anterior no pisa una corrección (ni su linaje) |
| 4 | Reejecución | Aplicar dos veces el mismo lote deja **idéntico** el estado completo (Silver, rechazados, control de lotes y Gold, valor por valor), y se verifica que la primera aplicación sí cambió algo |
| 5 | Duplicado | Dos filas idénticas en un lote no duplican el pago |
| 6 | Conflicto | Error controlado: `ConflictoLoteError`, estado intacto y auditoría `FAILED` |
| 7 | Fallo parcial | Un fallo justo antes del `COMMIT` (con el nuevo estado ya escrito dentro de la transacción) conserva el estado anterior, y el `SUCCESS` provisional desaparece |
| 8 | Gold actualizado | Una corrección de monto (C002) y una de estado (C010) se reflejan en `monto_pagado_90d` |

### Prueba de calidad integral (`tests/test_calidad.py`)

Reconcilia `lote_001` completo contra fuentes **independientes** del pipeline:

1. `registros_entrada` = filas del CSV contadas por Python (no por el pipeline).
2. `entrada = insertados + actualizados + ignorados + rechazados`.
3. `silver_pagos` creció exactamente en `insertados`; las filas con
   `batch_id = lote_001` son `insertados + actualizados`; los rechazados
   persistidos coinciden.
4. La fila de auditoría registró **los mismos** conteos que reportó el pipeline.
5. **Unicidad de `pago_id` en toda la tabla.** `silver_pagos` ya tiene
   `PRIMARY KEY`, pero la prueba protege contra que alguien la quite: un MERGE
   que inserte en lugar de actualizar dejaría la versión vieja y la nueva del
   mismo pago, y Gold sumaría ambas. Se verifica sobre toda la tabla porque un
   duplicado incremental siempre cruza lotes.

### Pruebas de desarrollo

`test_ingest`, `test_transform`, `test_gold`, `test_pipeline` (Semana 3) y
`test_lote_pagos`, `test_clasificacion`, `test_merge`,
`test_pipeline_incremental` (Semana 4) prueban cada pieza por separado con
datos sucios a propósito: si una regla nunca se ve fallar, no se sabe si
funciona. Las de aceptación son la cara visible; estas son la red de seguridad.

### ¿Las pruebas detectan errores? (sabotajes)

Se introdujeron defectos a propósito para comprobar que alguna prueba falla:

| # | Sabotaje | Consecuencia | Detectado por |
|---|---|---|---|
| 1 | Quitar `WHERE accion IN (...)` del MERGE | Versiones antiguas pisan correcciones | 5 pruebas (**ejecutado en CI**, ver abajo) |
| 2 | Clasificar `version_antigua` como posterior | Igual que el 1 | Aceptación 3 |
| 3 | Quitar el `DISTINCT` del lote | Un duplicado inocente se ve como conflicto | Aceptación 5 |
| 4 | No lanzar el conflicto del lote | Se elige una versión al azar | Aceptación 6 |
| 5 | `ROLLBACK` → `COMMIT` | Se confirma un estado a medias y la auditoría dice SUCCESS | Aceptación 7 |
| 6 | No filtrar `estado = 'EXITOSO'` en Gold | Pagos fallidos suman al monto | `test_gold` y aceptación 8 |

El sabotaje 6 **sobrevivió** a la primera versión de la prueba 8: usaba C002,
cuyos pagos son todos exitosos, así que el resultado era igual con o sin el
bug. Se corrigió agregando el caso de C010 (P024 `FALLIDO` → `EXITOSO`), donde
el estado sí importa.

---

## Integración continua

`.github/workflows/tests.yml` se ejecuta en cada **push**, en cada **pull
request** y manualmente desde la pestaña *Actions*:

1. Descarga el repositorio en un Ubuntu limpio.
2. Instala Python **3.10** (la misma versión del entorno de desarrollo).
3. Instala `requirements.txt` (con caché de pip).
4. Muestra las versiones instaladas, como evidencia de reproducibilidad.
5. Ejecuta `python -m pytest -ra`.
6. Construye la imagen Docker y ejecuta las pruebas dentro del contenedor.

Ejecuciones: <https://github.com/nnthony/customer360/actions>

![CI en verde](docs/evidencia/ci_verde.png)

### Regresión detectada automáticamente

Para comprobar que el CI protege la lógica del MERGE, en el
[pull request #1](https://github.com/nnthony/customer360/pull/1) se comentó
intencionalmente el filtro del `USING`:

```sql
    -- WHERE c.accion IN ('INSERT', 'UPDATE')
```

El workflow marcó el PR con **"All checks have failed"** (`5 failed, 127 passed`):

| Prueba | Qué detectó |
|---|---|
| `test_aceptacion::test_3_version_antigua` | P003 quedó en 999: la versión antigua pisó la corrección |
| `test_merge::test_lote_002_version_antigua_no_sobrescribe` | P024 volvió a `FALLIDO` |
| `test_pipeline_incremental::test_secuencia_completa_actualiza_gold` | El error llegó a Gold: C010 = 980 en vez de 1080 |
| `test_merge::test_linaje_solo_cambia_en_lo_que_se_escribio` | Un pago idéntico fue reescrito: el linaje (`batch_id`) quedó falso |
| `test_merge::test_el_orden_de_los_lotes_no_cambia_el_resultado` | **El resultado pasó a depender del orden de llegada** |

El último hallazgo es el más importante: `fecha_actualizacion` existe para que
aplicar `lote_001 → lote_002` dé lo mismo que `lote_002 → lote_001`. Sin el
filtro, P003 termina en 170 o en 180 según el orden. Una sola línea rompió la
propiedad central del diseño, y una prueba lo detectó antes de llegar a `main`.

El PR se cerró **sin fusionar**.

![PR con checks fallidos](docs/evidencia/pr_regresion.png)
![Log de pytest en el PR](docs/evidencia/log_regresion.png)

---

## Docker

El `Dockerfile` permite ejecutar las pruebas sin instalar Python:

```powershell
docker build -t customer360 .
docker run --rm customer360
```

Para ejecutar el pipeline y conservar los resultados en tu carpeta `data/`:

```powershell
docker run --rm -v "${PWD}/data:/app/data" customer360 python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_inicial.csv data/raw/pagos/pagos_lote_001.csv data/raw/pagos/pagos_lote_002.csv
```

La imagen se construye y prueba también en GitHub Actions (job `docker`), así
que funciona aunque Docker no esté instalado localmente.

---

## Qué se versiona y qué no

**Regla:** en Git va lo que **no se puede regenerar**. Lo que el pipeline
produce, no.

| Se versiona (fuente) | No se versiona (`.gitignore`) |
|---|---|
| `src/`, `sql/`, `tests/` | `data/bronze/`, `data/silver/`, `data/gold/` (salidas) |
| `data/raw/` (entrada de ejemplo) | `*.duckdb`, `*.duckdb.wal` (estado local) |
| `tests/fixtures/` | `data/logs/`, `*.tmp` |
| `requirements.txt`, `pytest.ini` | `.venv/`, `env/` (entornos virtuales) |
| `.github/workflows/`, `Dockerfile` | `__pycache__/`, `.pytest_cache/`, `.ruff_cache/` |

Versionar artefactos generados es dañino: un Parquet de una ejecución antigua
no corresponde al código actual, cada ejecución ensucia el diff y dos ramas que
ejecuten el pipeline chocan con archivos binarios imposibles de fusionar.

Bronze conserva datos, pero se conserva en el **almacenamiento de datos** (en
una empresa, un data lake), no en el repositorio de código.

---

## Decisiones de diseño

1. **La base DuckDB es el estado oficial.** Un solo `COMMIT` cubre Silver, Gold y
   la auditoría. Los Parquet/CSV son copias derivadas.
2. **Silver incremental y Gold completo.** Simplificación deliberada para no
   introducir incrementalidad en dos capas a la vez.
3. **La carga inicial es un lote más.** Pasa por el mismo MERGE contra un estado
   vacío: no hay un camino especial que pueda divergir.
4. **Misma data con fecha posterior → `UPDATE`, no `IGNORE`.** Si el estado tiene
   `P024 FALLIDO @ 20/09` y llega `P024 FALLIDO @ 25/09`, ignorarlo dejaría la
   fecha en 20/09; una versión intermedia `EXITOSO @ 22/09` que llegue después
   ganaría por error. Avanzar la fecha mantiene correcto el orden de versiones.
5. **Columnas de negocio vs. técnicas.** Solo las de negocio deciden si un
   registro cambió. `batch_id` es linaje.
6. **Conflicto estricto.** Dos versiones distintas con la misma fecha hacen fallar
   el lote completo, aunque exista una versión más reciente: un origen que manda
   dos verdades para el mismo instante no es confiable. Nunca se elige al azar.
7. **Clasificación con ramas explícitas, sin `ELSE`.** Un caso no previsto deja la
   acción en `NULL` y el pipeline falla indicando que la clasificación tiene un
   hueco, en lugar de etiquetarlo como algo que no es.
8. **Decidir y ejecutar están separados.** `lote_clasificado` es la única fuente
   de verdad: el MERGE ejecuta y la auditoría cuenta lo mismo.
9. **Rechazados reemplazados por lote** (`DELETE` + `INSERT` por `batch_id`): si se
   acumularan, reejecutar un lote los duplicaría.
10. **Clientes, servicios y contactos se cargan dentro de la transacción**, antes de
    validar el lote: la regla `cliente_inexistente` usa los mismos clientes que se
    van a confirmar.
11. **Bronze append-only + `control_lotes` con sha256:** un `batch_id` identifica un
    contenido único.
12. **Fecha de corte obligatoria y sin valor por defecto**, validada antes de tocar
    un archivo (*fail fast*).
13. **Python 3.10 fijado** en local y en CI. Fijar la versión fija el
    comportamiento: en 3.11, `date.fromisoformat()` acepta formatos que 3.10
    rechaza.
14. **Dependencias directas, sin `pip freeze`.**

---

## Respuesta a observaciones anteriores

| Observación | Cómo se resolvió |
|---|---|
| La publicación no es atómica | La base DuckDB es el estado oficial y Silver, Gold y la auditoría se confirman en una sola transacción. Los archivos exportados son copias, escritas con nombre temporal + `os.replace` |
| La deduplicación incorporaba decisiones de negocio no justificadas | En pagos, la precedencia la decide `fecha_actualizacion`, y un empate con contenido distinto **falla** en lugar de elegirse arbitrariamente. Ver limitaciones para servicios |
| Bronze no conservaba versiones históricas | Los lotes de pagos se guardan append-only en `bronze/pagos/<batch_id>.csv` y nunca se sobrescriben |
| `requirements.txt` en UTF-16 y la instalación no estaba explicada | Reescrito a mano en UTF-8 con dependencias directas; instalación documentada y verificada desde un clon limpio y en CI |

---

## Limitaciones y siguientes pasos

- **Solo pagos es incremental.** Clientes, servicios y contactos se recargan
  completos en cada ejecución.
- **La deduplicación de servicios** (carga completa) todavía prioriza el mayor cargo
  mensual. Necesita un criterio de precedencia real (fecha de actualización o
  versión del origen), como el de pagos.
- **No hay eliminaciones:** un pago borrado en el origen permanece en el estado.
  Requeriría marcas de borrado (*tombstones*).
- **Gold se reconstruye entero:** simple y correcto a este volumen, pero no escala a
  millones de clientes.
- **DuckDB admite un solo proceso escritor**; no hay ejecuciones concurrentes.
- **La fecha de corte es manual** y no hay orquestador (Airflow, cron).
- Git puede convertir los fines de línea en Windows; los CSV se leen igual, pero
  su sha256 puede diferir entre sistemas operativos.