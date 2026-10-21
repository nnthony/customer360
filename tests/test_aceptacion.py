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
    return consultar(
        base, "SELECT COUNT(*) FROM silver_pagos WHERE pago_id = ?", [pago_id]
    )[0][0]


def pago(base: Path, pago_id: str) -> dict | None:
    filas = consultar(
        base,
        """
        SELECT cliente_id, fecha_pago, monto, estado, fecha_actualizacion, batch_id
        FROM silver_pagos WHERE pago_id = ?
    """,
        [pago_id],
    )
    if not filas:
        return None
    columnas = [
        "cliente_id",
        "fecha_pago",
        "monto",
        "estado",
        "fecha_actualizacion",
        "batch_id",
    ]
    return dict(zip(columnas, filas[0]))


def monto_90d(base: Path, cliente_id: str):
    return consultar(
        base,
        "SELECT monto_pagado_90d FROM gold_customer360 WHERE cliente_id = ?",
        [cliente_id],
    )[0][0]


def estado_completo(base: Path) -> dict:
    """Todo lo que debe quedar IDÉNTICO al reejecutar. La auditoría queda fuera a
    propósito: es historia y cada ejecución debe dejar su fila."""
    return {
        "silver_pagos": consultar(base, "SELECT * FROM silver_pagos ORDER BY pago_id"),
        "rechazados": consultar(
            base, "SELECT * FROM silver_pagos_rechazados ORDER BY batch_id, pago_id"
        ),
        "control_lotes": consultar(
            base,
            "SELECT batch_id, archivo, sha256 FROM control_lotes ORDER BY batch_id",
        ),
        "gold": consultar(base, "SELECT * FROM gold_customer360 ORDER BY cliente_id"),
    }


def ultima_auditoria(base: Path) -> tuple:
    return consultar(
        base,
        """
        SELECT batch_id, estado, mensaje_error FROM auditoria_ejecuciones
        ORDER BY fecha_inicio DESC LIMIT 1
    """,
    )[0]


# ==========================================================================
# Las 8 pruebas de aceptación
# ==========================================================================


def test_1_insercion(base):
    """Un pago nuevo aparece exactamente una vez."""
    # Dado: P101 no existe
    assert pago(base, "P101") is None

    # Cuando: llega un lote con P101
    aplicar(
        base,
        lote(base, "lote_101", "P101,C004,2026-09-28,120,EXITOSO,2026-10-01 09:00:00"),
    )

    # Entonces: existe una sola vez, con sus valores
    assert contar(base, "P101") == 1
    assert pago(base, "P101")["monto"] == 120


def test_2_correccion(base):
    """Un pago existente cambia de monto al recibir una versión posterior."""
    # Dado: el monto de P003 es 200
    assert pago(base, "P003")["monto"] == 200

    # Cuando: Llega una correcion para P003
    aplicar(
        base,
        lote(base, "lote_102", "P003,C002,2026-09-10,150,EXITOSO,2026-09-11 10:00:00"),
    )
    # Entonces el monto se modifica con la ultima fila y queda 150
    assert contar(base, "P003") == 1
    assert pago(base, "P003")["monto"] == 150


def test_3_version_antigua(base):
    """Una versión anterior no sobrescribe una corrección reciente."""
    # Dado: ingresa una fila con fecha 2026-10-05
    aplicar(
        base,
        lote(base, "lote_101", "P003,C002,2026-09-10,150,EXITOSO,2026-10-05 10:00:00"),
    )

    assert pago(base, "P003")["batch_id"] == "lote_101"

    # Cuando: ingresa otra fila con fecha anterior
    aplicar(
        base,
        lote(base, "lote_102", "P003,C002,2026-09-10,999,EXITOSO,2026-10-01 10:00:00"),
    )

    # Entonces: no se inserta la fila antigua
    assert pago(base, "P003")["monto"] == 150
    assert pago(base, "P003")["batch_id"] == "lote_101"


def test_4_reejecucion(base):
    """Aplicar dos veces el mismo lote deja idéntico el estado completo."""

    fila1 = "P090,C004,2026-09-28,150,EXITOSO,2026-10-05 10:00:00"
    fila2 = "P003,C002,2026-09-10,180,EXITOSO,2026-10-06 10:00:00"
    fila3 = "P085,C081,2026-09-28,150,EXITOSO,2026-10-05 10:00:00"

    # Crear el lote UNA SOLA VEZ
    ruta = lote(base, "lote_108", fila1, fila2, fila3)

    # Cuando: se aplica el lote por primera vez
    aplicar(base, ruta)
    primer_estado = estado_completo(base)

    # (la 1ª aplicación SÍ hizo algo; si no, la comparación no prueba nada)
    assert contar(base, "P090") == 1
    assert pago(base, "P003")["monto"] == 180
    assert consultar(
        base, "SELECT pago_id FROM silver_pagos_rechazados WHERE batch_id = 'lote_108'"
    ) == [("P085",)]

    # Cuando: se aplica OTRA VEZ el mismo lote
    resultado = aplicar(base, ruta)

    # Entonces: el estado es idéntico y no se aplicó nada nuevo
    assert estado_completo(base) == primer_estado
    assert (resultado["registros_insertados"], resultado["registros_actualizados"]) == (
        0,
        0,
    )


def test_5_duplicado(base):
    """Dos filas idénticas dentro de un lote no duplican el pago."""
    # Dado: un lote tiene dos filas iguales
    fila = "P102,C004,2026-09-28,999,EXITOSO,2026-10-01 10:00:00"

    ruta = lote(base, "lote_102", fila, fila)

    # Cuando: se aplica el lote con duplicado
    resultado = aplicar(base, ruta)

    assert contar(base, "P102") == 1

    # Entonces: se inserta una y se ignora la otra
    assert resultado["registros_insertados"] == 1
    assert resultado["registros_ignorados"] == 1


def test_6_conflicto(base):
    """Dos versiones incompatibles con igual fecha generan un error controlado."""
    # Dado: un lote con 2 filas incompatibles
    fila1 = "P003,C002,2026-09-27,180,EXITOSO,2026-10-01 09:00:00"
    fila2 = "P003,C002,2026-09-27,120,EXITOSO,2026-10-01 09:00:00"

    ruta = lote(base, "lote_108", fila1, fila2)

    primer_estado = estado_completo(base)

    # Cuando: se ingresa ese lote con registros incompatibles
    with pytest.raises(ConflictoLoteError):
        aplicar(base, ruta)

    segundo_estado = estado_completo(base)

    # Entonces: no se ingresan las filas y se registra el error
    assert primer_estado == segundo_estado
    assert ultima_auditoria(base)[:2] == ("lote_108", "FAILED")

    assert "ConflictoLoteError" in ultima_auditoria(base)[2]


def test_7_fallo_parcial(base):
    """Una excepción antes del COMMIT conserva el estado anterior."""

    def fallar(con):
        batch_antes_commit = con.execute(
            "SELECT batch_id, estado FROM auditoria_ejecuciones ORDER BY fecha_inicio DESC"
        ).fetchall()[0]
        assert batch_antes_commit == ("lote_101", "SUCCESS")
        raise FalloSimuladoError("corte de luz")

    primer_estado = estado_completo(base)

    with pytest.raises(FalloSimuladoError):
        aplicar(
            base,
            lote(
                base, "lote_101", "P003,C002,2026-09-10,150,EXITOSO,2026-10-05 10:00:00"
            ),
            antes_de_commit=fallar,
        )

    assert ultima_auditoria(base)[:2] == ("lote_101", "FAILED")

    # Entonces: el SUCCESS que existía dentro de la transacción fue deshecho
    estados = consultar(base, "SELECT estado FROM auditoria_ejecuciones WHERE batch_id = 'lote_101'")
    assert estados == [("FAILED",)]

    segundo_estado = estado_completo(base)
    assert primer_estado == segundo_estado


def test_8_gold_actualizado(base):
    """Una corrección de pago se refleja en el monto agregado del cliente."""
    monto_1 = monto_90d(base, "C002")
    assert monto_1 == 300

    aplicar(
        base,
        lote(base, "lote_101", "P003,C002,2026-09-10,50,EXITOSO,2026-09-12 10:00:00"),
    )

    monto_2 = monto_90d(base, "C002")

    assert monto_2 == 150

    # Dado: C010 tiene un pago FALLIDO (P024, S/100) que NO suma
    assert monto_90d(base, "C010") == 980

    # Cuando: P024 se corrige a EXITOSO
    aplicar(base, lote(base, "lote_102", "P024,C010,2026-09-20,100,EXITOSO,2026-10-01 10:00:00"))

    # Entonces: ahora sí suma
    assert monto_90d(base, "C010") == 1080