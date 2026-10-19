"""
Tests de desarrollo del Día 4: MERGE transaccional y recuperación ante fallos.
Usan una base DuckDB REAL en disco (tmp_path), como en producción.
"""

from pathlib import Path

import pytest

from src.incremental import (
    RAIZ_PROYECTO,
    ConflictoLoteError,
    FalloSimuladoError,
    abrir_estado,
    aplicar_lote,
)

LOTES = RAIZ_PROYECTO / "data" / "raw" / "pagos"
INICIAL = LOTES / "pagos_inicial.csv"
LOTE_001 = LOTES / "pagos_lote_001.csv"
LOTE_002 = LOTES / "pagos_lote_002.csv"
ENCABEZADO = "pago_id,cliente_id,fecha_pago,monto,estado,fecha_actualizacion"


def nuevo_estado(carpeta: Path):
    con = abrir_estado(carpeta / "estado.duckdb")
    con.execute(
        "CREATE OR REPLACE TABLE silver_clientes AS "
        "SELECT 'C0' || LPAD(i::VARCHAR, 2, '0') AS cliente_id FROM range(1, 11) t(i)"
    )
    return con


@pytest.fixture
def con(tmp_path):
    c = nuevo_estado(tmp_path)
    yield c
    c.close()


def aplicar(con, tmp_path, *lotes, **kw):
    resultado = None
    for lote in lotes:
        resultado = aplicar_lote(con, lote, tmp_path / "bronze", **kw)
    return resultado


def pagos(con) -> dict:
    """Valores de NEGOCIO del estado (sin batch_id)."""
    filas = con.execute("""
        SELECT pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion
        FROM silver_pagos ORDER BY pago_id
    """).fetchall()
    return {f[0]: f[1:] for f in filas}


def estado_completo(con) -> tuple:
    """TODO lo que debe quedar idéntico al reejecutar: pagos con linaje + rechazados."""
    return (
        con.execute("SELECT * FROM silver_pagos ORDER BY pago_id").fetchall(),
        con.execute("SELECT * FROM silver_pagos_rechazados ORDER BY batch_id, pago_id").fetchall(),
    )


# --------------------------------------------------------------------------
# Aplicación normal
# --------------------------------------------------------------------------
def test_carga_inicial(con, tmp_path):
    r = aplicar(con, tmp_path, INICIAL)
    assert (r["registros_insertados"], r["registros_actualizados"], r["registros_ignorados"]) == (30, 0, 0)
    assert len(pagos(con)) == 30


def test_lote_001_aplica_inserciones_y_correcciones(con, tmp_path):
    r = aplicar(con, tmp_path, INICIAL, LOTE_001)
    p = pagos(con)

    assert (r["registros_insertados"], r["registros_actualizados"],
            r["registros_ignorados"], r["registros_rechazados"]) == (2, 2, 3, 1)
    assert len(p) == 32
    assert p["P003"][2] == 180                 # corrección de monto
    assert p["P024"][3] == "EXITOSO"           # corrección de estado
    assert "P033" not in p                     # rechazado (cliente inexistente)


def test_linaje_solo_cambia_en_lo_que_se_escribio(con, tmp_path):
    aplicar(con, tmp_path, INICIAL, LOTE_001)
    linaje = dict(con.execute("SELECT pago_id, batch_id FROM silver_pagos").fetchall())
    assert linaje["P003"] == "lote_001"        # actualizado
    assert linaje["P031"] == "lote_001"        # insertado
    assert linaje["P001"] == "inicial"         # ignorado: no se tocó


def test_lote_002_version_antigua_no_sobrescribe(con, tmp_path):
    aplicar(con, tmp_path, INICIAL, LOTE_001, LOTE_002)
    p = pagos(con)
    assert len(p) == 33
    assert p["P003"][2] == 170                 # segunda corrección
    assert p["P024"][3] == "EXITOSO"           # el FALLIDO del 25/09 llegó tarde: ignorado


def test_rechazados_quedan_registrados(con, tmp_path):
    aplicar(con, tmp_path, INICIAL, LOTE_001)
    rech = con.execute("SELECT batch_id, pago_id, motivo_rechazo FROM silver_pagos_rechazados").fetchall()
    assert rech == [("lote_001", "P033", "cliente_inexistente")]


# --------------------------------------------------------------------------
# Idempotencia
# --------------------------------------------------------------------------
def test_reaplicar_un_lote_deja_el_estado_identico(con, tmp_path):
    aplicar(con, tmp_path, INICIAL, LOTE_001)
    una_vez = estado_completo(con)

    r = aplicar(con, tmp_path, LOTE_001)

    assert estado_completo(con) == una_vez
    assert (r["registros_insertados"], r["registros_actualizados"]) == (0, 0)


def test_el_orden_de_los_lotes_no_cambia_el_resultado(tmp_path):
    """Gracias a fecha_actualizacion: Aplicar(L1, L2) == Aplicar(L2, L1)
    en los valores de negocio. (El linaje sí puede diferir, y está bien.)"""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a, b = nuevo_estado(tmp_path / "a"), nuevo_estado(tmp_path / "b")
    try:
        aplicar(a, tmp_path / "a", INICIAL, LOTE_001, LOTE_002)
        aplicar(b, tmp_path / "b", INICIAL, LOTE_002, LOTE_001)
        assert pagos(a) == pagos(b)
    finally:
        a.close()
        b.close()


# --------------------------------------------------------------------------
# Recuperación ante fallos
# --------------------------------------------------------------------------
def test_fallo_antes_del_commit_conserva_el_estado_anterior(con, tmp_path):
    aplicar(con, tmp_path, INICIAL)
    antes = estado_completo(con)

    def fallar(c):
        # Prueba de que el fallo ocurre en el PEOR momento:
        # dentro de la transacción, el nuevo estado YA está escrito...
        assert c.execute("SELECT monto FROM silver_pagos WHERE pago_id = 'P003'").fetchone()[0] == 180
        raise FalloSimuladoError("corte de luz simulado")

    with pytest.raises(FalloSimuladoError):
        aplicar(con, tmp_path, LOTE_001, antes_de_commit=fallar)

    # ...y aun así, tras el ROLLBACK, no queda rastro.
    assert estado_completo(con) == antes
    lotes = [f[0] for f in con.execute("SELECT batch_id FROM control_lotes").fetchall()]
    assert lotes == ["inicial"]


def test_reintento_despues_de_un_fallo_funciona(con, tmp_path):
    aplicar(con, tmp_path, INICIAL)

    def fallar(c):
        raise FalloSimuladoError("fallo transitorio")

    with pytest.raises(FalloSimuladoError):
        aplicar(con, tmp_path, LOTE_001, antes_de_commit=fallar)

    r = aplicar(con, tmp_path, LOTE_001)       # reintento sin fallo
    assert (r["registros_insertados"], r["registros_actualizados"]) == (2, 2)
    assert pagos(con)["P003"][2] == 180


def test_conflicto_no_toca_el_estado(con, tmp_path):
    aplicar(con, tmp_path, INICIAL, LOTE_001)
    antes = estado_completo(con)

    lote = tmp_path / "pagos_lote_900.csv"
    lote.write_text(ENCABEZADO + "\nP031,C004,2026-09-28,110,EXITOSO,2026-10-01 09:00:00\n", encoding="utf-8")
    with pytest.raises(ConflictoLoteError):
        aplicar(con, tmp_path, lote)

    assert estado_completo(con) == antes