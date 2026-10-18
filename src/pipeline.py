"""
pipeline.py — Orquestador Raw -> Bronze -> Silver -> Gold

Responsabilidad: ejecutar las capas en orden, verificar que los conteos
cuadren entre capas, dejar un registro de cada ejecución y reportar
errores de forma clara.

Ejecutar desde la raíz del proyecto:
    python -m src.pipeline --fecha-corte 2026-09-30
    python -m src.pipeline --fecha-corte 2026-09-30 --debug   # traceback completo
"""

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from src.ingest import ingestar_todo
from src.transform import ORDEN_TABLAS, transformar_todo

# Ruta absoluta al SQL: funciona sin importar desde qué carpeta se ejecute.
RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
SQL_GOLD = RAIZ_PROYECTO / "sql" / "customer360.sql"

COLUMNAS_GOLD = [
    "cliente_id",
    "cantidad_servicios_activos",
    "cargo_mensual_total",
    "ultimo_pago_exitoso",
    "monto_pagado_90d",
    "contactos_30d",
    "es_elegible",
]


# ==========================================================================
# Parámetros
# ==========================================================================


def parsear_fecha(valor) -> date:
    """Convierte 'YYYY-MM-DD' a date con un mensaje de error útil."""
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(valor)
    except (TypeError, ValueError):
        raise ValueError(
            f"Fecha de corte inválida '{valor}': usa el formato YYYY-MM-DD"
        ) from None


# ==========================================================================
# Gold
# ==========================================================================


def preparar_conexion(con, silver_dir: Path, fecha_corte: date) -> None:
    """Expone Silver como vistas y la fecha de corte como tabla de parámetros.

    Así el archivo .sql solo habla de `clientes`, `pagos`, `parametros`...
    y no sabe nada de rutas. Es lo mismo que hace dbt con `ref()`.
    """
    for tabla in ORDEN_TABLAS:
        ruta = (silver_dir / f"{tabla}.parquet").as_posix()
        con.execute(f"CREATE OR REPLACE VIEW {tabla} AS SELECT * FROM '{ruta}'")

    # Igual que tu TMP_FECHA_CORTE de Oracle, pero el valor viene de afuera.
    con.execute("CREATE OR REPLACE TEMP TABLE parametros (fecha_corte DATE)")
    con.execute("INSERT INTO parametros VALUES (?)", [fecha_corte])


def verificar_gold(con) -> None:
    """Contrato de Gold. Si algo falla, NO se publica nada."""
    columnas = [
        d[0] for d in con.execute("SELECT * FROM customer360 LIMIT 0").description
    ]
    if columnas != COLUMNAS_GOLD:
        raise RuntimeError(
            f"Columnas de Gold inválidas.\n  esperadas: {COLUMNAS_GOLD}\n  recibidas: {columnas}"
        )

    filas, distintos, clientes = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT cliente_id), (SELECT COUNT(*) FROM clientes)
        FROM customer360
    """).fetchone()
    if filas != distintos:
        raise RuntimeError(
            f"Gold debe tener una fila por cliente: {filas} filas, {distintos} clientes distintos"
        )
    if filas != clientes:
        raise RuntimeError(
            f"Gold tiene {filas} filas pero Silver tiene {clientes} clientes"
        )

    # ultimo_pago_exitoso SÍ puede ser NULL (cliente sin pagos). El resto no.
    nulos = con.execute("""
        SELECT COUNT(*) FROM customer360
        WHERE cliente_id IS NULL OR cantidad_servicios_activos IS NULL
           OR cargo_mensual_total IS NULL OR monto_pagado_90d IS NULL
           OR contactos_30d IS NULL OR es_elegible IS NULL
    """).fetchone()[0]
    if nulos:
        raise RuntimeError(
            f"Gold tiene {nulos} filas con NULL en columnas obligatorias (¿falta un COALESCE?)"
        )


def construir_gold(
    silver_dir: Path, gold_dir: Path, fecha_corte, sql_path: Path = SQL_GOLD
) -> dict:
    """Ejecuta customer360.sql sobre Silver, verifica y publica Gold."""
    fecha_corte = parsear_fecha(fecha_corte)
    silver_dir, gold_dir = Path(silver_dir), Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)

    # rstrip(";"): el archivo puede terminar en ';', pero dentro de CREATE ... AS no se permite.
    sql = Path(sql_path).read_text(encoding="utf-8").strip().rstrip(";")

    con = duckdb.connect()
    try:
        preparar_conexion(con, silver_dir, fecha_corte)
        con.execute(f"CREATE OR REPLACE TEMP TABLE customer360 AS {sql}")
        verificar_gold(con)  # Write-Audit-Publish: auditar ANTES de publicar

        for formato, opciones in [
            ("parquet", "FORMAT PARQUET"),
            ("csv", "HEADER, DELIMITER ','"),
        ]:
            ruta = (gold_dir / f"customer360.{formato}").as_posix()
            con.execute(
                f"COPY (SELECT * FROM customer360 ORDER BY cliente_id) TO '{ruta}' ({opciones})"
            )

        filas, elegibles = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE es_elegible) FROM customer360"
        ).fetchone()
    finally:
        con.close()

    return {
        "fecha_corte": fecha_corte.isoformat(),
        "filas": filas,
        "elegibles": elegibles,
    }


# ==========================================================================
# Reconciliación entre capas  <-- TU TAREA
# ==========================================================================


def reconciliar(log: dict) -> None:
    """Verifica que los conteos cuadren ENTRE capas.

    Cada capa ya se verifica a sí misma por dentro. Esto revisa las costuras:
    que lo que sale de una capa sea exactamente lo que entra a la siguiente.

    Junta TODOS los problemas y los reporta juntos (mejor que arreglar uno,
    volver a correr, y descubrir el siguiente).
    """
    errores = []

    for tabla, filas_bronze in log["bronze"].items():
        s = log["silver"][tabla]

        # TODO 1: lo que Silver leyó (s["entrada"]) debe ser igual a lo que Bronze guardó.
        #   Si no, agrega un mensaje a `errores`, ej:
        #   f"{tabla}: bronze={filas_bronze} pero silver leyó {s['entrada']}"
        if filas_bronze != s["entrada"]:
            errores.append(
                f"{tabla}: bronze={filas_bronze} pero silver leyó {s['entrada']}"
            )

        # TODO 2: s["validos"] + s["rechazados"] debe ser igual a s["entrada"].
        #   (Ninguna fila desaparece en silencio.)
        if s["entrada"] != s["validos"] + s["rechazados"]:
            errores.append(
                f"{tabla}: La cantidad de validos y rechazados suma {s['validos'] + s['rechazados']} pero silver tiene de entrada {s['entrada']}"
            )

        # TODO 3 (bonus): sum(s["motivos"].values()) debe ser igual a s["rechazados"].
        if s["rechazados"] != sum(s["motivos"].values()):
            errores.append(
                f"{tabla}: La cantidad de rechazados es {s['rechazados']} pero la cantidad de motivos suma {sum(s['motivos'].values())}"
            )

    # TODO 4: si ya existe log["gold"], sus filas deben ser iguales a los
    #   clientes válidos de Silver (log["silver"]["clientes"]["validos"]).
    #   Pista: if "gold" in log: ...
    if "gold" in log:
        if log["gold"]["filas"] != log["silver"]["clientes"]["validos"]:
            errores.append(
                f"gold leyó {log['gold']['filas']} filas, pero silver tiene {log['silver']['clientes']['validos']} clientes validos"
            )

    if errores:
        raise RuntimeError("Reconciliación fallida:\n  - " + "\n  - ".join(errores))


# ==========================================================================
# Registro de ejecución (punto 6 de la tarea)
# ==========================================================================


def guardar_log(log: dict, logs_dir: Path) -> Path:
    """Guarda el log de ESTA ejecución. Los logs se acumulan: son historia."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    ruta = logs_dir / f"run_{marca}.json"
    ruta.write_text(json.dumps(log, indent=2, ensure_ascii=False), encoding="utf-8")
    return ruta


def run(base_dir: Path, fecha_corte, sql_path: Path = SQL_GOLD) -> dict:
    """Pipeline completo. `base_dir` contiene raw/, bronze/, silver/, gold/, logs/.

    El log se guarda SIEMPRE, también si la ejecución falla: el día que algo
    se rompa, el log es lo primero que vas a querer leer.
    """
    base_dir = Path(base_dir)
    t0 = time.perf_counter()
    log = {
        "inicio": datetime.now(timezone.utc).isoformat(),
        "fecha_corte": str(fecha_corte),
        "estado": "en_curso",
    }
    try:
        # Fail fast: validar parámetros ANTES de tocar un solo archivo.
        fecha = parsear_fecha(fecha_corte)
        log["fecha_corte"] = fecha.isoformat()

        log["bronze"] = ingestar_todo(base_dir / "raw", base_dir / "bronze")
        log["silver"] = transformar_todo(base_dir / "bronze", base_dir / "silver")
        reconciliar(log)  # costura Bronze -> Silver, antes de construir Gold

        log["gold"] = construir_gold(
            base_dir / "silver", base_dir / "gold", fecha, sql_path
        )
        reconciliar(log)  # costura Silver -> Gold

        log["estado"] = "exitoso"
        return log
    except Exception as e:
        log["estado"] = "fallido"
        log["error"] = f"{type(e).__name__}: {e}"
        raise  # registrar NO es silenciar: el error sigue subiendo
    finally:
        log["duracion_seg"] = round(time.perf_counter() - t0, 3)
        log["archivo_log"] = str(guardar_log(log, base_dir / "logs"))


# ==========================================================================
# Línea de comandos
# ==========================================================================


def imprimir_resumen(log: dict) -> None:
    for tabla, n in log["bronze"].items():
        print(f"[bronze] {tabla:<10} {n:>4} filas")
    for tabla, r in log["silver"].items():
        print(
            f"[silver] {tabla:<10} validos={r['validos']:>4}  rechazados={r['rechazados']:>3}  {r['motivos']}"
        )
    g = log["gold"]
    print(
        f"[gold]   customer360 {g['filas']:>3} filas  elegibles={g['elegibles']}  (corte {g['fecha_corte']})"
    )
    print(f"[ok]     {log['duracion_seg']} s  ->  {log['archivo_log']}")


def main(argv=None) -> int:
    """Punto de entrada. Devuelve el código de salida (0 = ok, 1 = error)."""
    parser = argparse.ArgumentParser(description="Pipeline Customer 360")
    parser.add_argument(
        "--fecha-corte",
        required=True,
        help="Fecha de referencia YYYY-MM-DD para las ventanas de 30 y 90 días",
    )
    parser.add_argument(
        "--base-dir",
        default=str(RAIZ_PROYECTO / "data"),
        help="Carpeta con raw/, bronze/, silver/, gold/ (por defecto: data/)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Muestra el traceback completo"
    )
    args = parser.parse_args(argv)

    try:
        log = run(Path(args.base_dir), args.fecha_corte)
    except Exception as e:
        # Este es el ÚNICO lugar donde atrapamos "todo": el borde del programa.
        # No es para esconder el error, es para mostrarlo bien a una persona.
        if args.debug:
            raise
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        print(f"Detalle en: {Path(args.base_dir) / 'logs'}", file=sys.stderr)
        return 1

    imprimir_resumen(log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
