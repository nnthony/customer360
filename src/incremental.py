"""
incremental.py — Silver incremental de pagos (Semana 4)

Flujo de un lote:
  1. guardar_lote_en_bronze  -> copia INMUTABLE en bronze/pagos/<batch_id>.csv
  2. preparar_lote           -> valida, tipa y deduplica DENTRO del lote
                                (deja la tabla temporal `lote_staging`:
                                 una fila por pago_id, lista para el MERGE)
  3. (Día 3) clasificar contra el estado: INSERT / UPDATE / IGNORE / CONFLICT
  4. (Día 4) MERGE dentro de una transacción

Regla de precedencia entre versiones de un mismo pago_id:
  - gana la de mayor fecha_actualizacion;
  - misma fecha y mismo contenido -> duplicado (se conserva una);
  - misma fecha y distinto contenido -> CONFLICTO: el lote completo falla.
  Nunca se elige una versión arbitrariamente.
"""

import re
import shutil
from pathlib import Path
from datetime import datetime

import duckdb

from src.ingest import calcular_sha256

RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
SQL_SCHEMA = RAIZ_PROYECTO / "sql" / "schema.sql"
SQL_CLASIFICAR = RAIZ_PROYECTO / "sql" / "clasificar_pagos.sql"
SQL_MERGE = RAIZ_PROYECTO / "sql" / "merge_pagos.sql"

COLUMNAS_LOTE_PAGOS = [
    "pago_id",
    "cliente_id",
    "fecha_pago",
    "monto",
    "estado",
    "fecha_actualizacion",
]


class ConflictoLoteError(Exception):
    """Dos versiones del mismo pago con igual fecha_actualizacion y distinto contenido."""


# ==========================================================================
# Estado y Bronze
# ==========================================================================


def abrir_estado(db_path: Path):
    """Abre (o crea) la base oficial y asegura que existan las tablas."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(SQL_SCHEMA.read_text(encoding="utf-8"))
    return con


def batch_id_desde_archivo(ruta) -> str:
    """'pagos_lote_001.csv' -> 'lote_001'. Nombres fuera de convención se rechazan."""
    m = re.fullmatch(r"pagos_([a-z0-9_]+)", Path(ruta).stem)
    if not m:
        raise ValueError(
            f"Nombre de lote inválido '{Path(ruta).name}': se espera pagos_<batch_id>.csv"
        )
    return m.group(1)


def validar_columnas_lote(ruta: Path) -> None:
    columnas = duckdb.sql(
        f"SELECT * FROM read_csv('{Path(ruta).as_posix()}', all_varchar = true) LIMIT 0"
    ).columns
    if columnas != COLUMNAS_LOTE_PAGOS:
        raise ValueError(
            f"Columnas inválidas en {Path(ruta).name}\n"
            f"  esperadas: {COLUMNAS_LOTE_PAGOS}\n"
            f"  recibidas: {columnas}"
        )


def guardar_lote_en_bronze(ruta_lote: Path, bronze_dir: Path) -> dict:
    """Bronze append-only: un archivo por lote, NUNCA se sobrescribe.

    - Si el lote no existe en Bronze: se copia.
    - Si existe con el mismo hash: es una reejecución, no se toca.
    - Si existe con OTRO hash: alguien reutilizó el batch_id con otro contenido -> error.
    """
    ruta_lote = Path(ruta_lote)
    if not ruta_lote.exists():
        raise FileNotFoundError(f"No existe el lote {ruta_lote}")
    validar_columnas_lote(ruta_lote)

    batch_id = batch_id_desde_archivo(ruta_lote)
    destino = Path(bronze_dir) / "pagos" / f"{batch_id}.csv"
    hash_origen = calcular_sha256(ruta_lote)

    if destino.exists():
        if calcular_sha256(destino) != hash_origen:
            raise ValueError(
                f"El lote '{batch_id}' ya existe en Bronze con otro contenido. "
                f"Un batch_id no puede reutilizarse: usa un nuevo identificador."
            )
    else:
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ruta_lote, destino)

    return {"batch_id": batch_id, "ruta_bronze": destino, "sha256": hash_origen}


# ==========================================================================
# Preparación del lote (validar -> tipar -> deduplicar dentro del lote)
# ==========================================================================


def sql_validar(ruta_bronze: str) -> str:
    """Mismas reglas de pagos de la Semana 3 + fecha_actualizacion.
    Requiere la tabla `silver_clientes` (carga completa) para la integridad referencial."""
    return f"""
    CREATE OR REPLACE TEMP TABLE lote_validado AS
    WITH base AS (
        SELECT
            TRIM(pago_id)             AS pago_id,
            TRIM(cliente_id)          AS cliente_id,
            TRIM(fecha_pago)          AS fecha_pago,
            TRIM(monto)               AS monto,
            UPPER(TRIM(estado))       AS estado,
            TRIM(fecha_actualizacion) AS fecha_actualizacion
        FROM read_csv('{ruta_bronze}', all_varchar = true)
    )
    SELECT *,
        CASE
            WHEN pago_id IS NULL
              OR NOT regexp_matches(pago_id, '^P[0-9]{{3}}$')                    THEN 'id_invalido'
            WHEN cliente_id IS NULL
              OR cliente_id NOT IN (SELECT cliente_id FROM silver_clientes)      THEN 'cliente_inexistente'
            WHEN TRY_CAST(fecha_pago AS DATE) IS NULL                            THEN 'fecha_invalida'
            WHEN TRY_CAST(monto AS DECIMAL(12, 2)) IS NULL
              OR TRY_CAST(monto AS DECIMAL(12, 2)) <= 0                          THEN 'monto_invalido'
            WHEN estado IS NULL OR estado NOT IN ('EXITOSO', 'FALLIDO')          THEN 'estado_invalido'
            -- Sin fecha_actualizacion válida no hay forma de ordenar versiones.
            WHEN TRY_CAST(fecha_actualizacion AS TIMESTAMP) IS NULL              THEN 'fecha_actualizacion_invalida'
        END AS motivo_rechazo
    FROM base
    """


SQL_TIPAR = """
    CREATE OR REPLACE TEMP TABLE lote_tipado AS
    SELECT
        pago_id,
        cliente_id,
        CAST(fecha_pago AS DATE)              AS fecha_pago,
        CAST(monto AS DECIMAL(12, 2))         AS monto,
        estado,
        CAST(fecha_actualizacion AS TIMESTAMP) AS fecha_actualizacion
    FROM lote_validado
    WHERE motivo_rechazo IS NULL
"""

SQL_VERSIONES = """
    CREATE OR REPLACE TEMP TABLE lote_versiones AS 
    SELECT DISTINCT pago_id,cliente_id,fecha_pago,monto,estado,fecha_actualizacion FROM lote_tipado
"""

SQL_CONFLICTOS = """
    SELECT pago_id, fecha_actualizacion, count(1) Q FROM lote_versiones
    GROUP BY pago_id, fecha_actualizacion HAVING COUNT(1) >1
"""

SQL_STAGING = """
   CREATE OR REPLACE TEMP TABLE lote_staging AS
   SELECT * FROM lote_versiones
   -- Determinístico: SQL_CONFLICTOS ya garantizó que no hay dos versiones
   -- con el mismo (pago_id, fecha_actualizacion), así que el máximo es único.
   QUALIFY ROW_NUMBER() OVER (PARTITION BY pago_id ORDER BY fecha_actualizacion DESC) = 1
"""


def preparar_lote(con, ruta_bronze: Path, batch_id: str) -> dict:
    """Valida y deduplica un lote. Deja `lote_validado` y `lote_staging` en `con`.

    Lanza ConflictoLoteError si hay versiones incompatibles: el lote NO se aplica.
    """
    con.execute(sql_validar(Path(ruta_bronze).as_posix()))
    con.execute(SQL_TIPAR)
    con.execute(SQL_VERSIONES)

    conflictos = con.execute(SQL_CONFLICTOS).fetchall()
    if conflictos:
        detalle = "; ".join(f"{p} @ {f} ({n} versiones)" for p, f, n in conflictos)
        raise ConflictoLoteError(
            f"Lote '{batch_id}' rechazado por conflictos: {detalle}"
        )

    con.execute(SQL_STAGING)

    entrada = con.execute("SELECT COUNT(*) FROM lote_validado").fetchone()[0]
    validos = con.execute("SELECT COUNT(*) FROM lote_tipado").fetchone()[0]
    staging = con.execute("SELECT COUNT(*) FROM lote_staging").fetchone()[0]
    motivos = dict(
        con.execute("""
        SELECT motivo_rechazo, COUNT(*) FROM lote_validado
        WHERE motivo_rechazo IS NOT NULL GROUP BY 1 ORDER BY 1
    """).fetchall()
    )

    return {
        "batch_id": batch_id,
        "entrada": entrada,
        "rechazados": entrada - validos,
        "motivos": motivos,
        "ignorados_en_lote": validos
        - staging,  # duplicados exactos + versiones viejas del mismo lote
        "staging": staging,
    }


def clasificar_lote(con, batch_id: str) -> dict:
    """Clasifica cada fila de `lote_staging` contra `silver_pagos`.
 
    Deja la tabla temporal `lote_clasificado` (con columnas `detalle` y `accion`).
    NO modifica el estado. Lanza ConflictoLoteError si alguna fila choca con
    la versión vigente (misma fecha_actualizacion, contenido distinto).
    """
    con.execute(SQL_CLASIFICAR.read_text(encoding="utf-8"))
 
    sin_clasificar = con.execute(
        "SELECT COUNT(*) FROM lote_clasificado WHERE accion IS NULL"
    ).fetchone()[0]
    if sin_clasificar:
        raise RuntimeError(f"{sin_clasificar} filas sin clasificar: revisa el CASE de clasificar_pagos.sql")
 
    conflictos = con.execute("""
        SELECT pago_id, fecha_actualizacion FROM lote_clasificado
        WHERE accion = 'CONFLICT' ORDER BY pago_id
    """).fetchall()
    if conflictos:
        detalle = "; ".join(f"{p} @ {f}" for p, f in conflictos)
        raise ConflictoLoteError(
            f"Lote '{batch_id}' rechazado: contradice versiones vigentes con la misma "
            f"fecha_actualizacion: {detalle}"
        )
 
    acciones = dict(con.execute("SELECT accion, COUNT(*) FROM lote_clasificado GROUP BY 1").fetchall())
    detalle = dict(con.execute(
        "SELECT detalle, COUNT(*) FROM lote_clasificado GROUP BY 1 ORDER BY 1"
    ).fetchall())
    return {
        "insertados": acciones.get("INSERT", 0),
        "actualizados": acciones.get("UPDATE", 0),
        "ignorados_vs_estado": acciones.get("IGNORE", 0),
        "detalle": detalle,
    }
 
 
def conteos_auditoria(prep: dict, clas: dict) -> dict:
    """Une los conteos de preparar_lote y clasificar_lote en los campos de auditoría
    y verifica que cuadren: entrada = insertados + actualizados + ignorados + rechazados."""
    c = {
        "registros_entrada": prep["entrada"],
        "registros_insertados": clas["insertados"],
        "registros_actualizados": clas["actualizados"],
        "registros_ignorados": prep["ignorados_en_lote"] + clas["ignorados_vs_estado"],
        "registros_rechazados": prep["rechazados"],
    }
    suma = (c["registros_insertados"] + c["registros_actualizados"]
            + c["registros_ignorados"] + c["registros_rechazados"])
    if suma != c["registros_entrada"]:
        raise RuntimeError(f"Lote '{prep['batch_id']}': la auditoría no cuadra: entrada={c['registros_entrada']}, suma={suma}")
    return c

class FalloSimuladoError(Exception):
    """Error inyectado a propósito para probar la recuperación ante fallos."""


def registrar_lote(con, batch_id: str, archivo: str, sha256: str) -> None:
    """Segunda línea de defensa (la primera es Bronze): un batch_id ya aplicado
    con otro contenido se rechaza aunque alguien haya borrado data/bronze/."""
    fila = con.execute("SELECT sha256 FROM control_lotes WHERE batch_id = ?", [batch_id]).fetchone()
    if fila is None:
        con.execute(
            "INSERT INTO control_lotes VALUES (?, ?, ?, ?)",
            [batch_id, archivo, sha256, datetime.now()],
        )
    elif fila[0] != sha256:
        raise ValueError(f"El lote '{batch_id}' ya fue aplicado con otro contenido (sha256 distinto)")


def ejecutar_merge(con, batch_id: str, conteos: dict) -> None:
    """Ejecuta merge_pagos.sql y verifica que el resultado sea el que se decidió."""
    antes = con.execute("SELECT COUNT(*) FROM silver_pagos").fetchone()[0]

    con.execute("CREATE OR REPLACE TEMP TABLE parametros_lote (batch_id VARCHAR)")
    con.execute("INSERT INTO parametros_lote VALUES (?)", [batch_id])
    con.execute(SQL_MERGE.read_text(encoding="utf-8"))

    # Post-condición 1: el estado creció exactamente en los INSERT decididos.
    despues = con.execute("SELECT COUNT(*) FROM silver_pagos").fetchone()[0]
    if despues - antes != conteos["registros_insertados"]:
        raise RuntimeError(
            f"MERGE de '{batch_id}': se esperaban {conteos['registros_insertados']} filas nuevas "
            f"y el estado pasó de {antes} a {despues}"
        )

    # Post-condición 2: cada INSERT/UPDATE quedó escrito con la versión del lote.
    aplicados = con.execute("""
        SELECT COUNT(*)
        FROM lote_clasificado c
        JOIN silver_pagos e USING (pago_id)
        WHERE c.accion IN ('INSERT', 'UPDATE')
          AND e.fecha_actualizacion = c.fecha_actualizacion
          AND e.batch_id = (SELECT batch_id FROM parametros_lote)
    """).fetchone()[0]
    esperados = conteos["registros_insertados"] + conteos["registros_actualizados"]
    if aplicados != esperados:
        raise RuntimeError(f"MERGE de '{batch_id}': {aplicados} de {esperados} cambios quedaron aplicados")


def reemplazar_rechazados(con, batch_id: str) -> None:
    """DELETE + INSERT por batch_id: reaplicar el lote no duplica los rechazados."""
    con.execute("DELETE FROM silver_pagos_rechazados WHERE batch_id = ?", [batch_id])
    con.execute("""
        INSERT INTO silver_pagos_rechazados
            (batch_id, pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion, motivo_rechazo)
        SELECT ?, pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion, motivo_rechazo
        FROM lote_validado
        WHERE motivo_rechazo IS NOT NULL
    """, [batch_id])


def aplicar_lote(con, ruta_lote: Path, bronze_dir: Path, antes_de_commit=None) -> dict:
    """Aplica un lote de pagos al estado de forma atómica.

    Fuera de la transacción (solo lecturas y tablas temporales):
        Bronze -> preparar -> clasificar -> conteos.
        Un conflicto se detecta aquí, ANTES de tocar el estado.
    Dentro de la transacción (todo o nada):
        registrar lote -> MERGE -> rechazados -> [antes_de_commit] -> COMMIT

    `antes_de_commit`: función opcional que recibe `con` y se ejecuta justo antes
    del COMMIT. Los tests la usan para inyectar un fallo en el peor momento posible:
    con el nuevo estado ya escrito, pero todavía sin confirmar.
    """
    bronze = guardar_lote_en_bronze(ruta_lote, bronze_dir)
    batch_id = bronze["batch_id"]

    prep = preparar_lote(con, bronze["ruta_bronze"], batch_id)
    clas = clasificar_lote(con, batch_id)
    conteos = conteos_auditoria(prep, clas)


    con.execute("BEGIN TRANSACTION")
    try:
        registrar_lote(con, batch_id, Path(ruta_lote).name, bronze["sha256"])
        ejecutar_merge(con, batch_id, conteos)
        reemplazar_rechazados(con, batch_id)
        if antes_de_commit:
            antes_de_commit(con)
        con.execute("COMMIT")

    except Exception as e:
        con.execute("ROLLBACK")
        raise
    


    return {"batch_id": batch_id, **conteos}