"""
Tests de desarrollo del Día 2: preparación de un lote de pagos.
(Las 8 pruebas de aceptación de la tarea irán en test_incremental.py, Día 6.)
"""

from pathlib import Path

import duckdb
import pytest

from src.incremental import (
    RAIZ_PROYECTO,
    ConflictoLoteError,
    abrir_estado,
    batch_id_desde_archivo,
    guardar_lote_en_bronze,
    preparar_lote,
)

LOTES = RAIZ_PROYECTO / "data" / "raw" / "pagos"
FIXTURES = RAIZ_PROYECTO / "tests" / "fixtures"
ENCABEZADO = "pago_id,cliente_id,fecha_pago,monto,estado,fecha_actualizacion"


@pytest.fixture
def con():
    """Conexión en memoria con los 10 clientes válidos (como si Silver ya los tuviera)."""
    c = duckdb.connect()
    c.execute("CREATE TABLE silver_clientes AS SELECT 'C0' || LPAD(i::VARCHAR, 2, '0') AS cliente_id FROM range(1, 11) t(i)")
    yield c
    c.close()


def escribir_lote(tmp_path: Path, nombre: str, filas: list[str]) -> Path:
    ruta = tmp_path / nombre
    ruta.write_text(ENCABEZADO + "\n" + "\n".join(filas) + "\n", encoding="utf-8")
    return ruta


# --------------------------------------------------------------------------
# Identificación y Bronze inmutable
# --------------------------------------------------------------------------
@pytest.mark.parametrize("nombre, esperado", [
    ("pagos_inicial.csv", "inicial"),
    ("pagos_lote_001.csv", "lote_001"),
])
def test_batch_id_desde_archivo(nombre, esperado):
    assert batch_id_desde_archivo(nombre) == esperado


def test_nombre_de_lote_invalido():
    with pytest.raises(ValueError):
        batch_id_desde_archivo("pagos.csv")


def test_bronze_no_sobrescribe_un_lote_con_otro_contenido(tmp_path):
    lote = escribir_lote(tmp_path, "pagos_lote_900.csv", ["P001,C001,2026-09-05,50,EXITOSO,2026-10-01 09:00:00"])
    bronze = tmp_path / "bronze"
    guardar_lote_en_bronze(lote, bronze)
    guardar_lote_en_bronze(lote, bronze)                 # mismo contenido: reejecución OK

    lote.write_text(lote.read_text(encoding="utf-8").replace(",50,", ",99,"), encoding="utf-8")
    with pytest.raises(ValueError, match="otro contenido"):
        guardar_lote_en_bronze(lote, bronze)
    assert ",50," in (bronze / "pagos" / "lote_900.csv").read_text(encoding="utf-8")


def test_lote_sin_fecha_actualizacion_falla(tmp_path):
    lote = tmp_path / "pagos_lote_901.csv"
    lote.write_text("pago_id,cliente_id,fecha_pago,monto,estado\nP001,C001,2026-09-05,50,EXITOSO\n", encoding="utf-8")
    with pytest.raises(ValueError, match="fecha_actualizacion"):
        guardar_lote_en_bronze(lote, tmp_path / "bronze")


def test_esquema_se_puede_aplicar_dos_veces(tmp_path):
    abrir_estado(tmp_path / "estado.duckdb").close()
    c = abrir_estado(tmp_path / "estado.duckdb")       # IF NOT EXISTS: no debe fallar
    tablas = {t[0] for t in c.execute("SHOW TABLES").fetchall()}
    c.close()
    assert {"silver_pagos", "silver_pagos_rechazados", "control_lotes", "auditoria_ejecuciones"} <= tablas


# --------------------------------------------------------------------------
# Preparación con los lotes reales
# --------------------------------------------------------------------------
def test_lote_inicial(con):
    r = preparar_lote(con, LOTES / "pagos_inicial.csv", "inicial")
    assert (r["entrada"], r["rechazados"], r["ignorados_en_lote"], r["staging"]) == (30, 0, 0, 30)


def test_lote_001(con):
    r = preparar_lote(con, LOTES / "pagos_lote_001.csv", "lote_001")
    assert (r["entrada"], r["rechazados"], r["ignorados_en_lote"], r["staging"]) == (8, 1, 2, 5)
    assert r["motivos"] == {"cliente_inexistente": 1}

    staging = dict(con.execute("SELECT pago_id, monto FROM lote_staging").fetchall())
    assert staging["P003"] == 180          # gana la versión de las 10:00, no la de las 09:00
    assert sorted(staging) == ["P001", "P003", "P024", "P031", "P032"]


def test_lote_002(con):
    r = preparar_lote(con, LOTES / "pagos_lote_002.csv", "lote_002")
    assert (r["entrada"], r["rechazados"], r["ignorados_en_lote"], r["staging"]) == (4, 0, 0, 4)


# --------------------------------------------------------------------------
# Conflictos dentro del lote
# --------------------------------------------------------------------------
def test_conflicto_en_el_lote_falla(con):
    with pytest.raises(ConflictoLoteError, match="P031"):
        preparar_lote(con, FIXTURES / "pagos_lote_conflicto.csv", "conflicto")


def test_conflicto_en_version_antigua_tambien_falla(con, tmp_path):
    """Decisión estricta: aunque exista una versión más reciente que 'resolvería'
    el empate, dos versiones incompatibles indican un origen inconsistente."""
    lote = escribir_lote(tmp_path, "pagos_lote_902.csv", [
        "P003,C002,2026-09-10,190,EXITOSO,2026-10-01 09:00:00",
        "P003,C002,2026-09-10,185,EXITOSO,2026-10-01 09:00:00",
        "P003,C002,2026-09-10,180,EXITOSO,2026-10-01 10:00:00",
    ])
    with pytest.raises(ConflictoLoteError, match="P003"):
        preparar_lote(con, lote, "lote_902")


def test_fecha_actualizacion_invalida_se_rechaza(con, tmp_path):
    lote = escribir_lote(tmp_path, "pagos_lote_903.csv", [
        "P001,C001,2026-09-05,50,EXITOSO,2026-10-01 09:00:00",
        "P002,C001,2026-09-05,50,EXITOSO,ayer",
    ])
    r = preparar_lote(con, lote, "lote_903")
    assert r["motivos"] == {"fecha_actualizacion_invalida": 1}
    assert r["staging"] == 1