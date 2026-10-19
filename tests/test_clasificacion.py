"""
Tests de desarrollo del Día 3: clasificación de un lote contra el estado.

El estado se "siembra" a mano con INSERT: todavía no existe el MERGE (Día 4),
y así cada test controla exactamente contra qué versión se compara.
"""

from pathlib import Path

import duckdb
import pytest

from src.incremental import (
    RAIZ_PROYECTO,
    SQL_SCHEMA,
    ConflictoLoteError,
    clasificar_lote,
    conteos_auditoria,
    preparar_lote,
)

LOTES = RAIZ_PROYECTO / "data" / "raw" / "pagos"
ENCABEZADO = "pago_id,cliente_id,fecha_pago,monto,estado,fecha_actualizacion"


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute(SQL_SCHEMA.read_text(encoding="utf-8"))
    c.execute("CREATE TABLE silver_clientes AS SELECT 'C0' || LPAD(i::VARCHAR, 2, '0') AS cliente_id FROM range(1, 11) t(i)")
    yield c
    c.close()


def sembrar(con, filas: list[tuple]) -> None:
    """Inserta versiones vigentes directamente en silver_pagos."""
    con.executemany(
        """INSERT INTO silver_pagos (pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion, batch_id)
           VALUES (?, ?, ?, ?, ?, ?, 'semilla')""",
        filas,
    )


def sembrar_desde_lote(con, ruta: Path, batch_id: str) -> None:
    preparar_lote(con, ruta, batch_id)
    con.execute(f"""
        INSERT INTO silver_pagos (pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion, batch_id)
        SELECT pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion, '{batch_id}'
        FROM lote_staging
    """)


def clasificar(con, tmp_path: Path, filas: list[str], batch_id="lote_900") -> dict:
    ruta = tmp_path / f"pagos_{batch_id}.csv"
    ruta.write_text(ENCABEZADO + "\n" + "\n".join(filas) + "\n", encoding="utf-8")
    preparar_lote(con, ruta, batch_id)
    return clasificar_lote(con, batch_id)


def acciones(con) -> dict:
    return dict(con.execute("SELECT pago_id, accion FROM lote_clasificado").fetchall())


# --------------------------------------------------------------------------
# Lotes reales
# --------------------------------------------------------------------------
def test_lote_001_contra_estado_inicial(con):
    sembrar_desde_lote(con, LOTES / "pagos_inicial.csv", "inicial")
    prep = preparar_lote(con, LOTES / "pagos_lote_001.csv", "lote_001")
    clas = clasificar_lote(con, "lote_001")

    assert acciones(con) == {
        "P031": "INSERT", "P032": "INSERT",
        "P003": "UPDATE", "P024": "UPDATE",
        "P001": "IGNORE",
    }
    assert clas["detalle"] == {"identico": 1, "nuevo": 2, "version_posterior": 2}
    assert conteos_auditoria(prep, clas) == {
        "registros_entrada": 8,
        "registros_insertados": 2,
        "registros_actualizados": 2,
        "registros_ignorados": 3,      # 2 dentro del lote + 1 idéntico al estado
        "registros_rechazados": 1,
    }


def test_lote_002_contra_estado_tras_lote_001(con, tmp_path):
    sembrar(con, [
        ("P003", "C002", "2026-09-10", 180, "EXITOSO", "2026-10-01 10:00:00"),
        ("P024", "C010", "2026-09-20", 100, "EXITOSO", "2026-10-01 10:00:00"),
        ("P031", "C004", "2026-09-28", 120, "EXITOSO", "2026-10-01 09:00:00"),
    ])
    preparar_lote(con, LOTES / "pagos_lote_002.csv", "lote_002")
    clas = clasificar_lote(con, "lote_002")

    assert acciones(con) == {"P003": "UPDATE", "P024": "IGNORE", "P034": "INSERT", "P031": "IGNORE"}
    assert clas["detalle"] == {"identico": 1, "nuevo": 1, "version_antigua": 1, "version_posterior": 1}


# --------------------------------------------------------------------------
# Reglas individuales
# --------------------------------------------------------------------------
def test_version_antigua_no_gana(con, tmp_path):
    sembrar(con, [("P024", "C010", "2026-09-20", 100, "EXITOSO", "2026-10-01 10:00:00")])
    clasificar(con, tmp_path, ["P024,C010,2026-09-20,100,FALLIDO,2026-09-25 10:00:00"])
    assert acciones(con) == {"P024": "IGNORE"}


def test_misma_data_con_fecha_posterior_es_update(con, tmp_path):
    """Decisión del Día 1: la fecha avanza aunque los valores no cambien,
    para que una versión intermedia que llegue después no gane por error."""
    sembrar(con, [("P024", "C010", "2026-09-20", 100, "FALLIDO", "2026-09-20 10:00:00")])
    clas = clasificar(con, tmp_path, ["P024,C010,2026-09-20,100,FALLIDO,2026-09-25 10:00:00"])
    assert acciones(con) == {"P024": "UPDATE"}
    assert clas["detalle"] == {"version_posterior": 1}


def test_conflicto_contra_el_estado_falla(con, tmp_path):
    sembrar(con, [("P031", "C004", "2026-09-28", 120, "EXITOSO", "2026-10-01 09:00:00")])
    with pytest.raises(ConflictoLoteError, match="P031"):
        clasificar(con, tmp_path, ["P031,C004,2026-09-28,110,EXITOSO,2026-10-01 09:00:00"])


@pytest.mark.parametrize("fila_distinta", [
    "P031,C005,2026-09-28,120,EXITOSO,2026-10-01 09:00:00",   # otro cliente
    "P031,C004,2026-09-27,120,EXITOSO,2026-10-01 09:00:00",   # otra fecha_pago
    "P031,C004,2026-09-28,120,FALLIDO,2026-10-01 09:00:00",   # otro estado
])
def test_cualquier_columna_de_negocio_distinta_es_conflicto(con, tmp_path, fila_distinta):
    sembrar(con, [("P031", "C004", "2026-09-28", 120, "EXITOSO", "2026-10-01 09:00:00")])
    with pytest.raises(ConflictoLoteError):
        clasificar(con, tmp_path, [fila_distinta])


def test_batch_id_distinto_no_es_cambio(con, tmp_path):
    """El estado tiene batch_id='semilla' y el lote otro: sigue siendo idéntico."""
    sembrar(con, [("P031", "C004", "2026-09-28", 120, "EXITOSO", "2026-10-01 09:00:00")])
    clas = clasificar(con, tmp_path, ["P031,C004,2026-09-28,120,EXITOSO,2026-10-01 09:00:00"])
    assert clas["detalle"] == {"identico": 1}


def test_clasificar_no_modifica_el_estado(con):
    sembrar_desde_lote(con, LOTES / "pagos_inicial.csv", "inicial")
    antes = con.execute("SELECT * FROM silver_pagos ORDER BY pago_id").fetchall()

    preparar_lote(con, LOTES / "pagos_lote_001.csv", "lote_001")
    clasificar_lote(con, "lote_001")

    assert con.execute("SELECT * FROM silver_pagos ORDER BY pago_id").fetchall() == antes