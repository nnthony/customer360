"""
pipeline_incremental.py — Orquestador de la Semana 4

Procesa UN lote de pagos por ejecución (run). El estado oficial es la base
DuckDB; los Parquet/CSV de gold/ son copias derivadas que se exportan al final.

    Fuera de la transacción (archivos, idempotentes):
        Bronze + Silver de cargas completas (clientes, servicios, contactos)
        Bronze append-only del lote de pagos
    BEGIN
        cargas completas -> silver_clientes / silver_servicios / silver_contactos
        preparar + clasificar el lote
        control_lotes + MERGE silver_pagos + rechazados
        reconstruir gold_customer360
        auditoría SUCCESS
    COMMIT                      <- Silver, Gold y auditoría se confirman JUNTOS
    exportar gold/ (Parquet, CSV)

    Si algo falla: ROLLBACK, y DESPUÉS (fuera de la transacción) auditoría FAILED.

Ejecutar desde la raíz:
    python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/pagos_inicial.csv
    python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote data/raw/pagos/*.csv
    python -m src.pipeline_incremental --fecha-corte 2026-09-30 --lote ... --simular-fallo
"""

import argparse
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import logging

from src import azure_storage as az
from src.publicar import PublicacionError, publicar

from src.incremental import (
    FalloSimuladoError,
    abrir_estado,
    batch_id_desde_archivo,
    clasificar_lote,
    conteos_auditoria,
    ejecutar_merge,
    guardar_lote_en_bronze,
    preparar_lote,
    reemplazar_rechazados,
    registrar_lote,
)
from src.ingest import ingestar_todo
from src.pipeline import RAIZ_PROYECTO, SQL_GOLD, parsear_fecha, verificar_gold
from src.transform import transformar_todo

TABLAS_CARGA_COMPLETA = ["clientes", "servicios", "contactos"]


# ==========================================================================
# Pasos dentro de la transacción
# ==========================================================================

def cargar_cargas_completas(con, silver_dir: Path) -> None:
    """Reemplaza las tablas de carga completa con el Silver recién validado.
    En DuckDB el DDL es transaccional: si hay ROLLBACK, vuelven las versiones anteriores."""
    for tabla in TABLAS_CARGA_COMPLETA:
        ruta = (Path(silver_dir) / f"{tabla}.parquet").as_posix()
        con.execute(f"CREATE OR REPLACE TABLE silver_{tabla} AS SELECT * FROM '{ruta}'")


def reconstruir_gold(con, fecha_corte, sql_path: Path = SQL_GOLD) -> dict:
    """Reconstruye Gold COMPLETO desde Silver con el MISMO customer360.sql de la Semana 3.

    El SQL habla de `clientes`, `pagos`, `parametros`...: aquí esos nombres se
    apuntan a las tablas del estado en vez de a archivos Parquet. El SQL no cambia.
    """
    for tabla in ("clientes", "servicios", "contactos", "pagos"):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {tabla} AS SELECT * FROM silver_{tabla}")
    con.execute("CREATE OR REPLACE TEMP TABLE parametros (fecha_corte DATE)")
    con.execute("INSERT INTO parametros VALUES (?)", [fecha_corte])

    sql = Path(sql_path).read_text(encoding="utf-8").strip().rstrip(";")
    con.execute(f"CREATE OR REPLACE TABLE gold_customer360 AS {sql}")

    # Mismo contrato de Gold de la Semana 3 (columnas, 1 fila por cliente, sin NULL).
    con.execute("CREATE OR REPLACE TEMP VIEW customer360 AS SELECT * FROM gold_customer360")
    verificar_gold(con)

    filas, elegibles = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE es_elegible) FROM gold_customer360"
    ).fetchone()
    return {"filas": filas, "elegibles": elegibles}


# ==========================================================================
# Auditoría
# ==========================================================================

def registrar_auditoria(con, run_id: str, batch_id: str, fecha_inicio: datetime, conteos: dict) -> None:
    """Fila SUCCESS. Se llama DENTRO de la transacción, justo antes del COMMIT."""
    con.execute(
        """
        INSERT INTO auditoria_ejecuciones (
            run_id,
            batch_id,
            fecha_inicio,
            fecha_fin,
            estado,
            registros_entrada,
            registros_insertados,
            registros_actualizados,
            registros_ignorados,
            registros_rechazados,
            mensaje_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            batch_id,
            fecha_inicio,
            datetime.now(),
            "SUCCESS",
            conteos.get("registros_entrada"),
            conteos.get("registros_insertados"),
            conteos.get("registros_actualizados"),
            conteos.get("registros_ignorados"),
            conteos.get("registros_rechazados"),
            None
        ]
    )


def registrar_fallo(con, run_id: str, batch_id: str, fecha_inicio: datetime,
                    conteos: dict, error: Exception) -> None:
    """Fila FAILED. Se llama DESPUÉS del ROLLBACK, fuera de la transacción de datos."""
    con.execute(
        """
        INSERT INTO auditoria_ejecuciones (
            run_id,
            batch_id,
            fecha_inicio,
            fecha_fin,
            estado,
            registros_entrada,
            registros_insertados,
            registros_actualizados,
            registros_ignorados,
            registros_rechazados,
            mensaje_error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            batch_id,
            fecha_inicio,
            datetime.now(),
            "FAILED",
            conteos.get("registros_entrada"),
            0,
            0,
            0,
            0,
            f"{type(error).__name__}: {error}"
        ]
    )



def _registrar_fallo_seguro(con, *args) -> None:
    """Si registrar el fallo falla (ej. disco lleno), NO debe tapar el error original."""
    try:
        registrar_fallo(con, *args)
    except Exception as e:
        print(f"ADVERTENCIA: no se pudo registrar el fallo en la auditoría: {e}", file=sys.stderr)


# ==========================================================================
# Exportación (después del COMMIT)
# ==========================================================================

def exportar_gold(con, gold_dir: Path) -> None:
    """Copias derivadas del estado oficial. Cada archivo se escribe primero con un
    nombre temporal y luego se renombra (os.replace): un lector nunca ve un archivo
    a medio escribir. Si la exportación falla, la base sigue correcta y basta con
    volver a exportar."""
    gold_dir = Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    for ext, opciones in [("parquet", "FORMAT PARQUET"), ("csv", "FORMAT CSV, HEADER")]:
        final = gold_dir / f"customer360.{ext}"
        temporal = gold_dir / f".customer360.{ext}.tmp"
        con.execute(
            f"COPY (SELECT * FROM gold_customer360 ORDER BY cliente_id) "
            f"TO '{temporal.as_posix()}' ({opciones})"
        )
        os.replace(temporal, final)


# ==========================================================================
# Orquestación de un lote
# ==========================================================================

def procesar_lote(base_dir: Path, ruta_lote: Path, fecha_corte, db_path: Path = None,
                  sql_path: Path = SQL_GOLD, antes_de_commit=None) -> dict:
    base_dir = Path(base_dir)
    db_path = Path(db_path) if db_path else base_dir / "customer360.duckdb"

    run_id = str(uuid.uuid4())
    inicio = datetime.now()
    batch_id = "desconocido"
    conteos: dict = {}

    con = abrir_estado(db_path)
    try:
        try:
            fecha = parsear_fecha(fecha_corte)
            batch_id = batch_id_desde_archivo(ruta_lote)

            # Archivos: idempotentes, no necesitan transacción.
            ingestar_todo(base_dir / "raw", base_dir / "bronze", tablas=TABLAS_CARGA_COMPLETA)
            transformar_todo(base_dir / "bronze", base_dir / "silver", tablas=TABLAS_CARGA_COMPLETA)
            bronze = guardar_lote_en_bronze(ruta_lote, base_dir / "bronze")

            con.execute("BEGIN TRANSACTION")
            try:
                cargar_cargas_completas(con, base_dir / "silver")

                prep = preparar_lote(con, bronze["ruta_bronze"], batch_id)
                clas = clasificar_lote(con, batch_id)
                conteos = conteos_auditoria(prep, clas)

                registrar_lote(con, batch_id, Path(ruta_lote).name, bronze["sha256"])
                ejecutar_merge(con, batch_id, conteos)
                reemplazar_rechazados(con, batch_id)

                gold = reconstruir_gold(con, fecha, sql_path)
                registrar_auditoria(con, run_id, batch_id, inicio, conteos)

                if antes_de_commit is not None:
                    antes_de_commit(con)
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        except Exception as e:
            _registrar_fallo_seguro(con, run_id, batch_id, inicio, conteos, e)
            raise

        exportar_gold(con, base_dir / "gold")
        return {"run_id": run_id, "batch_id": batch_id, **conteos, "gold": gold}
    finally:
        con.close()


# ==========================================================================
# Línea de comandos
# ==========================================================================

def _fallo_simulado(con) -> None:
    raise FalloSimuladoError("fallo simulado antes del COMMIT (--simular-fallo)")


def _publicar(base_dir: Path, fecha_corte: str, resultados: list, debug: bool) -> int:
    """Publica en el Data Lake. Se llama SOLO si todos los lotes terminaron en SUCCESS."""
    logging.getLogger("azure.identity").setLevel(logging.ERROR)
    ejecucion_id = str(uuid.uuid4())
    try:
        fs = az.obtener_contenedor()
        m = publicar(fs, base_dir, ejecucion_id, fecha_corte, resultados)
    except (az.AccesoAzureError, az.IntegridadError, PublicacionError) as e:
        if debug:
            raise
        print(f"[publicación] FAILED  {type(e).__name__}: {e}", file=sys.stderr)
        print("El estado local quedó confirmado; se puede volver a publicar.", file=sys.stderr)
        return 1
    print(f"[publicación] OK  {m['totales']['archivos']} archivos, {m['totales']['bytes']} bytes"
          f"  ->  _manifiestos/ejecucion_{ejecucion_id}.json")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline incremental Customer 360")
    parser.add_argument("--lote", nargs="+", required=True,
                        help="Uno o más CSV de pagos; se aplican en orden alfabético")
    parser.add_argument("--fecha-corte", required=True, help="YYYY-MM-DD")
    parser.add_argument("--base-dir", default=str(RAIZ_PROYECTO / "data"))
    parser.add_argument("--simular-fallo", action="store_true",
                        help="Lanza un error justo antes del COMMIT (prueba de recuperación)")
    parser.add_argument("--publicar", action="store_true",
                        help="Al terminar con éxito, publica en el Data Lake "
                             "(requiere AZURE_STORAGE_ACCOUNT y az login)")
    parser.add_argument("--debug", action="store_true", help="Muestra el traceback completo")
    args = parser.parse_args(argv)

    hook = _fallo_simulado if args.simular_fallo else None
    resultados = []
    for ruta in sorted(args.lote):
        try:
            r = procesar_lote(Path(args.base_dir), Path(ruta), args.fecha_corte, antes_de_commit=hook)
        except Exception as e:
            if args.debug:
                raise
            print(f"[{Path(ruta).name}] FAILED  {type(e).__name__}: {e}", file=sys.stderr)
            return 1          # un lote falló: se sale ANTES de llegar a publicar
        resultados.append(r)
        print(
            f"[{r['batch_id']:<9}] SUCCESS  entrada={r['registros_entrada']:>3}  "
            f"ins={r['registros_insertados']:>3}  upd={r['registros_actualizados']:>3}  "
            f"ign={r['registros_ignorados']:>3}  rech={r['registros_rechazados']:>3}  |  "
            f"gold {r['gold']['filas']} filas, elegibles={r['gold']['elegibles']}"
        )

    if args.publicar:
        return _publicar(Path(args.base_dir), args.fecha_corte, resultados, args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())