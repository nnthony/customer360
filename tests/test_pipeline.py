"""
Tests del orquestador (src/pipeline.py): ejecución completa, reconciliación,
registro de ejecución y manejo de errores en la línea de comandos.

Ejecutar desde la raíz:  python -m pytest -v
"""

import copy
import json
from pathlib import Path

import duckdb
import pytest

from src.pipeline import main, reconciliar, run

FECHA = "2026-09-30"

RAW_MINIMO = {
    "clientes": "cliente_id,fecha_alta,estado\nC001,2025-01-10,ACTIVO\nC002,2025-02-15,INACTIVO\n",
    "servicios": "servicio_id,cliente_id,tipo_servicio,estado,cargo_mensual\nS001,C001,INTERNET,ACTIVO,100\n",
    "pagos": (
        "pago_id,cliente_id,fecha_pago,monto,estado\n"
        "P001,C001,2026-09-05,50,EXITOSO\n"
        "P002,C999,2026-09-05,50,EXITOSO\n"          # cliente inexistente -> rechazado
    ),
    "contactos": "contacto_id,cliente_id,fecha_contacto,canal\nT001,C001,2026-09-01,EMAIL\n",
}


@pytest.fixture
def base(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    for tabla, contenido in RAW_MINIMO.items():
        (raw / f"{tabla}.csv").write_text(contenido, encoding="utf-8")
    return tmp_path


def logs_de(base: Path) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted((base / "logs").glob("run_*.json"))]


# --------------------------------------------------------------------------
# Ejecución completa
# --------------------------------------------------------------------------
def test_run_completo_genera_todas_las_capas(base):
    log = run(base, FECHA)

    assert log["estado"] == "exitoso"
    assert log["bronze"] == {"clientes": 2, "servicios": 1, "pagos": 2, "contactos": 1}
    assert log["silver"]["pagos"]["rechazados"] == 1
    assert log["gold"]["filas"] == 2
    assert log["gold"]["elegibles"] == 1          # C001 sí, C002 inactivo
    assert (base / "gold" / "customer360.parquet").exists()
    assert (base / "gold" / "customer360.csv").exists()


def test_ejecutar_desde_cero_dos_veces_da_el_mismo_gold(base):
    """Idempotencia de punta a punta (lo que pide el Día 7)."""
    ruta_gold = (base / "gold" / "customer360.parquet").as_posix()

    log_1 = run(base, FECHA)
    gold_1 = duckdb.sql(f"SELECT * FROM '{ruta_gold}' ORDER BY 1").fetchall()
    log_2 = run(base, FECHA)
    gold_2 = duckdb.sql(f"SELECT * FROM '{ruta_gold}' ORDER BY 1").fetchall()

    assert gold_1 == gold_2
    for capa in ("bronze", "silver", "gold"):
        assert log_1[capa] == log_2[capa]


# --------------------------------------------------------------------------
# Registro de ejecución
# --------------------------------------------------------------------------
def test_log_registra_filas_por_etapa(base):
    run(base, FECHA)
    [log] = logs_de(base)

    assert log["estado"] == "exitoso"
    assert log["fecha_corte"] == FECHA
    assert {"bronze", "silver", "gold", "inicio", "duracion_seg"} <= log.keys()
    assert log["silver"]["pagos"]["motivos"] == {"cliente_inexistente": 1}


def test_cada_ejecucion_deja_su_propio_log(base):
    run(base, FECHA)
    run(base, FECHA)
    assert len(logs_de(base)) == 2


def test_si_falla_igual_queda_log_con_el_error(base):
    (base / "raw" / "pagos.csv").unlink()
    with pytest.raises(FileNotFoundError):
        run(base, FECHA)

    [log] = logs_de(base)
    assert log["estado"] == "fallido"
    assert "pagos.csv" in log["error"]


# --------------------------------------------------------------------------
# Fail fast
# --------------------------------------------------------------------------
def test_fecha_invalida_falla_antes_de_tocar_archivos(base):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        run(base, "30/09/2026")
    assert not (base / "bronze").exists(), "No debería haberse procesado nada"


# --------------------------------------------------------------------------
# Reconciliación (unitarios con logs fabricados)
# --------------------------------------------------------------------------
LOG_CUADRA = {
    "bronze": {"clientes": 2, "pagos": 3},
    "silver": {
        "clientes": {"entrada": 2, "validos": 2, "rechazados": 0, "motivos": {}},
        "pagos": {"entrada": 3, "validos": 2, "rechazados": 1, "motivos": {"duplicado": 1}},
    },
    "gold": {"filas": 2},
}


def test_reconciliacion_ok_no_lanza():
    reconciliar(copy.deepcopy(LOG_CUADRA))


def test_reconciliacion_sin_gold_todavia():
    log = copy.deepcopy(LOG_CUADRA)
    del log["gold"]
    reconciliar(log)  # después de Silver, Gold aún no existe


def test_reconciliacion_detecta_bronze_distinto_de_silver():
    log = copy.deepcopy(LOG_CUADRA)
    log["silver"]["pagos"]["entrada"] = 4
    log["silver"]["pagos"]["validos"] = 3
    with pytest.raises(RuntimeError, match="pagos"):
        reconciliar(log)


def test_reconciliacion_detecta_filas_perdidas_en_silver():
    log = copy.deepcopy(LOG_CUADRA)
    log["silver"]["pagos"]["validos"] = 1          # 1 + 1 != 3
    with pytest.raises(RuntimeError, match="pagos"):
        reconciliar(log)


def test_reconciliacion_detecta_motivos_que_no_suman():
    log = copy.deepcopy(LOG_CUADRA)
    log["silver"]["pagos"]["motivos"] = {}
    with pytest.raises(RuntimeError, match="pagos"):
        reconciliar(log)


def test_reconciliacion_detecta_gold_distinto_de_clientes():
    log = copy.deepcopy(LOG_CUADRA)
    log["gold"]["filas"] = 3
    with pytest.raises(RuntimeError, match="(?i)gold"):
        reconciliar(log)


def test_reconciliacion_reporta_todos_los_errores_juntos():
    log = copy.deepcopy(LOG_CUADRA)
    log["silver"]["clientes"]["validos"] = 1       # error en clientes
    log["silver"]["pagos"]["validos"] = 1          # error en pagos
    with pytest.raises(RuntimeError) as exc:
        reconciliar(log)
    assert "clientes" in str(exc.value) and "pagos" in str(exc.value)


# --------------------------------------------------------------------------
# Línea de comandos
# --------------------------------------------------------------------------
def test_cli_exitosa_devuelve_0(base, capsys):
    codigo = main(["--fecha-corte", FECHA, "--base-dir", str(base)])
    salida = capsys.readouterr().out
    assert codigo == 0
    assert "customer360" in salida and "elegibles=1" in salida


def test_cli_con_error_devuelve_1_y_mensaje_claro(base, capsys):
    codigo = main(["--fecha-corte", "30/09/2026", "--base-dir", str(base)])
    err = capsys.readouterr().err
    assert codigo == 1
    assert err.startswith("ERROR:")
    assert "YYYY-MM-DD" in err
    assert "Traceback" not in err


def test_cli_modo_debug_muestra_la_excepcion(base):
    with pytest.raises(ValueError):
        main(["--fecha-corte", "30/09/2026", "--base-dir", str(base), "--debug"])


def test_cli_exige_fecha_de_corte(base):
    with pytest.raises(SystemExit) as exc:      # argparse sale con código 2
        main(["--base-dir", str(base)])
    assert exc.value.code == 2