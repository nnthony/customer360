"""
Pruebas de aceptación — Semana 4 (incrementales, idempotencia y recuperación)

Una prueba por cada caso obligatorio de la tarea, en el mismo orden.
Todas usan el punto de entrada real (procesar_lote) sobre una base DuckDB
en disco, partiendo del estado tras aplicar pagos_inicial.csv (30 pagos).

Cada lote de prueba es mínimo (1-2 filas) para que se vea a simple vista
QUÉ se está probando. Estructura de cada test:
    Dado   -> el estado inicial (fixture `base`)
    Cuando -> se aplica un lote
    Entonces -> lo que debe cumplirse

Ejecutar solo estas pruebas:
    python -m pytest -m aceptacion -v
"""

import shutil
from pathlib import Path

import duckdb
import pytest

from src.incremental import ConflictoLoteError, FalloSimuladoError
from src.pipeline import RAIZ_PROYECTO
from src.pipeline_incremental import procesar_lote

pytestmark = pytest.mark.aceptacion

RAW_REAL = RAIZ_PROYECTO / "data" / "raw"
FECHA = "2026-09-30"
ENCABEZADO = "pago_id,cliente_id,fecha_pago,monto,estado,fecha_actualizacion"


# ==========================================================================
# Plomería
# ==========================================================================

@pytest.fixture
def base(tmp_path: Path) -> Path:
    """Proyecto temporal con clientes/servicios/contactos reales y la carga inicial aplicada."""
    (tmp_path / "raw").mkdir()
    for tabla in ("clientes", "servicios", "contactos"):
        shutil.copy(RAW_REAL / f"{tabla}.csv", tmp_path / "raw" / f"{tabla}.csv")
    aplicar(tmp_path, RAW_REAL / "pagos" / "pagos_inicial.csv")
    return tmp_path


def lote(base: Path, batch_id: str, *filas: str) -> Path:
    """Escribe un lote mínimo pagos_<batch_id>.csv y devuelve su ruta."""
    carpeta = base / "entrada"
    carpeta.mkdir(exist_ok=True)
    ruta = carpeta / f"pagos_{batch_id}.csv"
    ruta.write_text(ENCABEZADO + "\n" + "\n".join(filas) + "\n", encoding="utf-8")
    return ruta


def aplicar(base: Path, ruta_lote: Path, **kw) -> dict:
    return procesar_lote(base, ruta_lote, FECHA, **kw)


def consultar(base: Path, sql: str, params=None) -> list:
    con = duckdb.connect(str(base / "customer360.duckdb"), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def contar(base: Path, pago_id: str) -> int:
    return consultar(base, "SELECT COUNT(*) FROM silver_pagos WHERE pago_id = ?", [pago_id])[0][0]


def pago(base: Path, pago_id: str) -> dict | None:
    filas = consultar(base, """
        SELECT cliente_id, fecha_pago, monto, estado, fecha_actualizacion, batch_id
        FROM silver_pagos WHERE pago_id = ?
    """, [pago_id])
    if not filas:
        return None
    columnas = ["cliente_id", "fecha_pago", "monto", "estado", "fecha_actualizacion", "batch_id"]
    return dict(zip(columnas, filas[0]))


def monto_90d(base: Path, cliente_id: str):
    return consultar(base, "SELECT monto_pagado_90d FROM gold_customer360 WHERE cliente_id = ?", [cliente_id])[0][0]


def estado_completo(base: Path) -> dict:
    """Todo lo que debe quedar IDÉNTICO al reejecutar. La auditoría queda fuera a
    propósito: es historia y cada ejecución debe dejar su fila."""
    return {
        "silver_pagos": consultar(base, "SELECT * FROM silver_pagos ORDER BY pago_id"),
        "rechazados": consultar(base, "SELECT * FROM silver_pagos_rechazados ORDER BY batch_id, pago_id"),
        "control_lotes": consultar(base, "SELECT batch_id, archivo, sha256 FROM control_lotes ORDER BY batch_id"),
        "gold": consultar(base, "SELECT * FROM gold_customer360 ORDER BY cliente_id"),
    }


def ultima_auditoria(base: Path) -> tuple:
    return consultar(base, """
        SELECT batch_id, estado, mensaje_error FROM auditoria_ejecuciones
        ORDER BY fecha_inicio DESC LIMIT 1
    """)[0]


# ==========================================================================
# Las 8 pruebas de aceptación
# ==========================================================================

def test_1_insercion(base):
    """Un pago nuevo aparece exactamente una vez."""
    # Dado: P101 no existe
    assert pago(base, "P101") is None

    # Cuando: llega un lote con P101
    aplicar(base, lote(base, "lote_101", "P101,C004,2026-09-28,120,EXITOSO,2026-10-01 09:00:00"))

    # Entonces: existe una sola vez, con sus valores
    assert contar(base, "P101") == 1
    assert pago(base, "P101")["monto"] == 120


def test_2_correccion(base):
    """Un pago existente cambia de monto al recibir una versión posterior."""
    # TODO: P003 existe con monto 200 y fecha_actualizacion 2026-09-10 10:00:00.
    #   Cuando llega P003 con monto 150 y una fecha_actualizacion posterior,
    #   el monto vigente debe ser 150 y seguir existiendo una sola vez.
    # pytest.fail("pendiente")
    aplicar(base, lote(base, "lote_102", "P003,C004,2026-09-28,150,EXITOSO,2026-09-11 10:00:00"))
    #aplicar(base, lote(base, "lote_102", "P003,C004,2026-09-29,150,EXITOSO,2026-09-11 11:00:00"))
    assert contar(base, "P003") == 1
    assert pago(base, "P003")["monto"] == 150


def test_3_version_antigua(base):
    """Una versión anterior no sobrescribe una corrección reciente."""
    # TODO: necesitas DOS lotes.
    #   1º una corrección reciente de P003 (ej. 150 @ 2026-10-05 10:00:00)
    #   2º una versión vieja que llega tarde (ej. 999 @ 2026-10-01 10:00:00)
    #   Entonces: P003 sigue en 150.
    #   Bonus: verifica también que su batch_id sigue siendo el del 1º lote.
    aplicar(base, lote(base, "lote_101", "P003,C004,2026-09-28,150,EXITOSO,2026-10-05 10:00:00"))
    aplicar(base, lote(base, "lote_102", "P003,C004,2026-09-28,999,EXITOSO,2026-10-01 10:00:00"))
    #pytest.fail("pendiente")
    assert pago(base, "P003")["monto"] == 150
    assert consultar(base, "SELECT batch_id FROM silver_pagos WHERE pago_id = ?", ["P003"])[0][0] =="lote_101"


def test_4_reejecucion(base):
    """Aplicar dos veces el mismo lote deja idéntico el estado completo."""
    # TODO: arma un lote "variado" (una inserción + una corrección + una fila
    #   con cliente inexistente para que haya rechazados), aplícalo, guarda
    #   estado_completo(), aplícalo otra vez y compara.
    #   Recuerda: el profe pide comparar VALORES, no solo cantidad de filas.
    #   Pista: estado_completo() ya hace eso por ti.
    #pytest.fail("pendiente")
    datos = "\n".join([
        # INSERT: pago nuevo con cliente existente
        "P090,C004,2026-09-28,150,EXITOSO,2026-10-05 10:00:00",

        # UPDATE: corrección más reciente de un pago existente
        "P003,C004,2026-09-28,180,EXITOSO,2026-10-06 10:00:00",

        # REJECT: cliente inexistente
        "P085,C081,2026-09-28,150,EXITOSO,2026-10-05 10:00:00",
    ])

    # Crear el lote UNA SOLA VEZ
    ruta = lote(base, "lote_108", datos)

    # Primera ejecución
    aplicar(base, ruta)
    primer_estado = estado_completo(base)

    # Segunda ejecución del MISMO lote
    aplicar(base, ruta)
    segundo_estado = estado_completo(base)

    # Los valores deben ser idénticos
    assert primer_estado == segundo_estado


def test_5_duplicado(base):
    """Dos filas idénticas dentro de un lote no duplican el pago."""
    # TODO: un lote con la MISMA fila dos veces (un pago nuevo, ej. P102).
    #   Entonces: P102 aparece una sola vez.
    #   Bonus: el resultado de aplicar() trae registros_insertados e ignorados;
    #   ¿cuánto debería valer cada uno?
    pytest.fail("pendiente")


def test_6_conflicto(base):
    """Dos versiones incompatibles con igual fecha generan un error controlado."""
    # TODO: un lote con P003 dos veces, MISMA fecha_actualizacion, montos distintos.
    #   "Error controlado" significa tres cosas; verifica las tres:
    #     a) se lanza ConflictoLoteError (no un error cualquiera) -> pytest.raises
    #     b) el estado no cambió                               -> estado_completo()
    #     c) quedó evidencia en la auditoría                   -> ultima_auditoria()
    pytest.fail("pendiente")


def test_7_fallo_parcial(base):
    """Una excepción antes del COMMIT conserva el estado anterior."""
    # TODO: guarda estado_completo(), aplica un lote con una corrección pasando
    #   antes_de_commit=<una función que lanza FalloSimuladoError>, y verifica:
    #     - que se lanzó FalloSimuladoError
    #     - que estado_completo() es idéntico al de antes
    #     - que la auditoría registró FAILED
    #   Bonus (el más valioso): haz que tu función, ANTES de lanzar el error,
    #   compruebe con `con.execute(...)` que la corrección SÍ estaba escrita
    #   dentro de la transacción. Así demuestras que el ROLLBACK deshizo algo real.
    pytest.fail("pendiente")


def test_8_gold_actualizado(base):
    """Una corrección de pago se refleja en el monto agregado del cliente."""
    # TODO: con corte 2026-09-30, C002 tiene monto_pagado_90d = 300
    #   (P003 200 + P004 100; P005 es de julio y queda fuera).
    #   Corrige P003 a 150 -> ¿cuánto debe valer ahora monto_90d(base, "C002")?
    #   Verifica el valor ANTES y DESPUÉS: así el test explica el cambio.
    pytest.fail("pendiente")