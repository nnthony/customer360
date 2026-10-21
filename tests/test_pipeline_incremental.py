"""
Tests de desarrollo del Día 5: pipeline incremental completo
(Silver + Gold + auditoría en una transacción).
"""

import shutil
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.incremental import ConflictoLoteError, FalloSimuladoError
from src.pipeline_incremental import main, procesar_lote
from src.pipeline import RAIZ_PROYECTO

RAW_REAL = RAIZ_PROYECTO / "data" / "raw"
LOTES = RAW_REAL / "pagos"
FIXTURES = RAIZ_PROYECTO / "tests" / "fixtures"
FECHA = "2026-09-30"


@pytest.fixture
def base(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    for tabla in ("clientes", "servicios", "contactos"):
        shutil.copy(RAW_REAL / f"{tabla}.csv", raw / f"{tabla}.csv")
    return tmp_path


def procesar(base: Path, *lotes: str, **kw):
    r = None
    for nombre in lotes:
        r = procesar_lote(base, LOTES / f"pagos_{nombre}.csv", FECHA, **kw)
    return r


def consultar(base: Path, sql: str) -> list:
    con = duckdb.connect(str(base / "customer360.duckdb"), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def gold(base: Path) -> dict:
    filas = consultar(
        base,
        "SELECT cliente_id, ultimo_pago_exitoso, monto_pagado_90d FROM gold_customer360",
    )
    return {f[0]: f[1:] for f in filas}


def auditoria(base: Path) -> list:
    return consultar(
        base,
        """
        SELECT batch_id, estado, registros_entrada, registros_insertados,
               registros_actualizados, registros_ignorados, registros_rechazados
        FROM auditoria_ejecuciones ORDER BY fecha_inicio
    """,
    )


# --------------------------------------------------------------------------
def test_secuencia_completa_actualiza_gold(base):
    procesar(base, "inicial", "lote_001", "lote_002")
    g = gold(base)
    assert g["C001"] == (date(2026, 9, 29), 100)
    assert g["C002"] == (date(2026, 9, 10), 270)
    assert g["C004"] == (date(2026, 9, 28), 120)
    assert g["C006"] == (date(2026, 9, 30), 435)
    assert g["C010"] == (date(2026, 9, 25), 1080)
    assert g["C003"] == (date(2026, 9, 15), 90)  # sin cambios


def test_auditoria_registra_cada_ejecucion(base):
    procesar(base, "inicial", "lote_001", "lote_002")
    assert auditoria(base) == [
        ("inicial", "SUCCESS", 30, 30, 0, 0, 0),
        ("lote_001", "SUCCESS", 8, 2, 2, 3, 1),
        ("lote_002", "SUCCESS", 4, 1, 1, 2, 0),
    ]


def test_reejecutar_lote_no_cambia_gold_pero_queda_auditado(base):
    procesar(base, "inicial", "lote_001")
    antes = gold(base)
    procesar(base, "lote_001")

    assert gold(base) == antes
    assert auditoria(base)[-1] == ("lote_001", "SUCCESS", 8, 0, 0, 7, 1)


def test_fallo_simulado_no_deja_rastro_salvo_la_auditoria(base):
    procesar(base, "inicial")
    gold_antes = gold(base)
    pagos_antes = consultar(base, "SELECT * FROM silver_pagos ORDER BY pago_id")

    def fallar(con):
        raise FalloSimuladoError("corte de luz")

    with pytest.raises(FalloSimuladoError):
        procesar(base, "lote_001", antes_de_commit=fallar)

    assert gold(base) == gold_antes
    assert consultar(base, "SELECT * FROM silver_pagos ORDER BY pago_id") == pagos_antes
    assert consultar(base, "SELECT batch_id FROM control_lotes") == [("inicial",)]

    batch, estado, mensaje = consultar(
        base,
        """
        SELECT batch_id, estado, mensaje_error FROM auditoria_ejecuciones ORDER BY fecha_inicio DESC LIMIT 1
    """,
    )[0]
    assert (batch, estado) == ("lote_001", "FAILED")
    assert "FalloSimuladoError" in mensaje

    # Los archivos exportados tampoco cambiaron: siguen reflejando el último COMMIT.
    parquet = (base / "gold" / "customer360.parquet").as_posix()
    assert (
        duckdb.sql(
            f"SELECT monto_pagado_90d FROM '{parquet}' WHERE cliente_id = 'C002'"
        ).fetchone()[0]
        == 300
    )


def test_conflicto_queda_auditado_como_fallido(base):
    procesar(base, "inicial", "lote_001")
    antes = gold(base)

    with pytest.raises(ConflictoLoteError):
        procesar_lote(base, FIXTURES / "pagos_lote_conflicto.csv", FECHA)

    assert gold(base) == antes
    ultimo = auditoria(base)[-1]
    assert ultimo[:2] == ("lote_conflicto", "FAILED")


def test_exportacion_coincide_con_el_estado(base):
    procesar(base, "inicial", "lote_001")
    parquet = (base / "gold" / "customer360.parquet").as_posix()
    exportado = duckdb.sql(f"SELECT * FROM '{parquet}' ORDER BY cliente_id").fetchall()
    assert exportado == consultar(
        base, "SELECT * FROM gold_customer360 ORDER BY cliente_id"
    )


# --------------------------------------------------------------------------
# Línea de comandos
# --------------------------------------------------------------------------
def test_cli_aplica_varios_lotes_en_orden(base, capsys):
    lotes = [
        str(LOTES / f"pagos_{n}.csv") for n in ("lote_002", "inicial", "lote_001")
    ]  # desordenados
    assert (
        main(["--lote", *lotes, "--fecha-corte", FECHA, "--base-dir", str(base)]) == 0
    )
    assert [f[0] for f in auditoria(base)] == ["inicial", "lote_001", "lote_002"]


def test_cli_simular_fallo_devuelve_1(base, capsys):
    codigo = main(
        [
            "--lote",
            str(LOTES / "pagos_inicial.csv"),
            "--fecha-corte",
            FECHA,
            "--base-dir",
            str(base),
            "--simular-fallo",
        ]
    )
    assert codigo == 1
    assert "FAILED" in capsys.readouterr().err
    assert consultar(base, "SELECT COUNT(*) FROM silver_pagos") == [(0,)]
    assert auditoria(base)[-1][:2] == ("inicial", "FAILED")
