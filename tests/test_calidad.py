"""
Prueba de calidad integral: reconciliación completa de un lote.

Ejecutar solo esta prueba:
    python -m pytest -m calidad -v
"""

import csv
import shutil
from pathlib import Path

import duckdb
import pytest

from src.pipeline import RAIZ_PROYECTO
from src.pipeline_incremental import procesar_lote

pytestmark = pytest.mark.calidad

RAW_REAL = RAIZ_PROYECTO / "data" / "raw"
LOTE_001 = RAW_REAL / "pagos" / "pagos_lote_001.csv"
FECHA = "2026-09-30"


@pytest.fixture
def base(tmp_path: Path) -> Path:
    """Proyecto temporal con clientes/servicios/contactos reales y la carga inicial aplicada."""
    (tmp_path / "raw").mkdir()
    for tabla in ("clientes", "servicios", "contactos"):
        shutil.copy(RAW_REAL / f"{tabla}.csv", tmp_path / "raw" / f"{tabla}.csv")
    procesar_lote(tmp_path, RAW_REAL / "pagos" / "pagos_inicial.csv", FECHA)
    return tmp_path


def consultar(base: Path, sql: str, params=None) -> list:
    con = duckdb.connect(str(base / "customer360.duckdb"), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def escalar(base: Path, sql: str, params=None):
    """Primera columna de la primera fila."""
    return consultar(base, sql, params)[0][0]


def test_reconciliacion_integral_de_un_lote(base):
    """Un lote real (inserciones, correcciones, repetidos y rechazos) cuadra en
    todas sus cuentas, contra fuentes independientes del propio pipeline, y el
    estado no queda con pagos duplicados.
    """
    # Dado: el estado tras la carga inicial
    pagos_antes = escalar(base, "SELECT COUNT(*) FROM silver_pagos")

    # Conteo de entrada con una fuente INDEPENDIENTE del pipeline: el CSV leído
    # por Python. Si el pipeline perdiera filas al leer, sus propios conteos
    # seguirían cuadrando entre sí; solo esta comparación lo detecta.
    with open(LOTE_001, encoding="utf-8", newline="") as archivo:
        filas_csv = sum(1 for _ in csv.DictReader(archivo))

    # Cuando: se aplica lote_001
    r = procesar_lote(base, LOTE_001, FECHA)
    entrada = r["registros_entrada"]
    insertados = r["registros_insertados"]
    actualizados = r["registros_actualizados"]
    ignorados = r["registros_ignorados"]
    rechazados = r["registros_rechazados"]

    # Entonces 1: el pipeline leyó todas las filas del archivo
    assert entrada == filas_csv

    # Entonces 2: ninguna fila desaparece en silencio
    assert entrada == insertados + actualizados + ignorados + rechazados

    # Entonces 3: los números corresponden a cambios REALES en las tablas
    assert escalar(base, "SELECT COUNT(*) FROM silver_pagos") - pagos_antes == insertados
    assert escalar(
        base, "SELECT COUNT(*) FROM silver_pagos WHERE batch_id = 'lote_001'"
    ) == insertados + actualizados
    assert escalar(
        base, "SELECT COUNT(*) FROM silver_pagos_rechazados WHERE batch_id = 'lote_001'"
    ) == rechazados

    # Entonces 4: lo que quedó REGISTRADO en la auditoría es lo que se REPORTÓ
    auditoria = consultar(base, """
        SELECT registros_entrada, registros_insertados, registros_actualizados,
               registros_ignorados, registros_rechazados
        FROM auditoria_ejecuciones
        WHERE batch_id = 'lote_001' AND estado = 'SUCCESS'
    """)
    assert auditoria == [(entrada, insertados, actualizados, ignorados, rechazados)]

    # Entonces 5: unicidad de pagos en TODO el estado (no solo en el lote)
    total, distintos = consultar(
        base, "SELECT COUNT(*), COUNT(DISTINCT pago_id) FROM silver_pagos"
    )[0]
    assert total == distintos

    # Valores concretos de lote_001 (documentan el caso; ver matriz del README)
    assert (entrada, insertados, actualizados, ignorados, rechazados) == (8, 2, 2, 3, 1)