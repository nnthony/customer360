"""
Tests de la capa Bronze (src/ingest.py).

Idea clave: NUNCA testeamos con data/raw real. Cada test crea sus propios
CSV mínimos en una carpeta temporal (tmp_path) que pytest borra al terminar.
Así los tests son rápidos, repetibles y no dependen de tu máquina.

Ejecutar desde la raíz del proyecto:
    python -m pytest -v
"""

import json
from pathlib import Path

import pytest

from src.ingest import (
    TABLAS_ESPERADAS,
    calcular_sha256,
    contar_filas,
    ingestar_todo,
)

# --------------------------------------------------------------------------
# Datos de prueba: pocas filas, fáciles de contar a ojo.
# --------------------------------------------------------------------------
CSV_PRUEBA = {
    "clientes": (
        "cliente_id,fecha_alta,estado\n"
        "C001,2025-01-10,ACTIVO\n"
        "C002,2025-02-15,INACTIVO\n"
    ),
    "servicios": (
        "servicio_id,cliente_id,tipo_servicio,estado,cargo_mensual\n"
        "S001,C001,INTERNET,ACTIVO,100\n"
        "S002,C001,TV,VENCIDO,50\n"
        "S003,C002,TV,CANCELADO,40\n"
    ),
    "pagos": (
        "pago_id,cliente_id,fecha_pago,monto,estado\nP001,C001,2026-09-05,50,EXITOSO\n"
    ),
    "contactos": (
        "contacto_id,cliente_id,fecha_contacto,canal\n"
        "T001,C001,2026-09-01,EMAIL\n"
        "T002,C002,2026-09-02,CHAT\n"
    ),
}
FILAS_ESPERADAS = {"clientes": 2, "servicios": 3, "pagos": 1, "contactos": 2}


@pytest.fixture
def dirs(tmp_path: Path):
    """Crea data/raw con los 4 CSV de prueba y devuelve (raw_dir, bronze_dir)."""
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    for tabla, contenido in CSV_PRUEBA.items():
        (raw_dir / f"{tabla}.csv").write_text(contenido, encoding="utf-8")
    return raw_dir, bronze_dir


# --------------------------------------------------------------------------
# Camino feliz
# --------------------------------------------------------------------------
def test_contrato_y_datos_de_prueba_estan_alineados():
    """Si alguien cambia TABLAS_ESPERADAS, este test avisa que hay que
    actualizar también los CSV de prueba."""
    assert set(CSV_PRUEBA) == set(TABLAS_ESPERADAS)


def test_devuelve_conteo_por_tabla(dirs):
    raw_dir, bronze_dir = dirs
    conteos = ingestar_todo(raw_dir, bronze_dir)
    assert conteos == FILAS_ESPERADAS


def test_contar_filas_no_cuenta_encabezado(dirs):
    raw_dir, _ = dirs
    assert contar_filas(raw_dir / "pagos.csv") == 1


def test_bronze_es_copia_exacta_byte_a_byte(dirs):
    raw_dir, bronze_dir = dirs
    ingestar_todo(raw_dir, bronze_dir)
    for tabla in TABLAS_ESPERADAS:
        original = (raw_dir / f"{tabla}.csv").read_bytes()
        copia = (bronze_dir / f"{tabla}.csv").read_bytes()
        assert copia == original, f"{tabla}.csv en bronze no es idéntico a raw"


def test_bronze_no_interpreta_valores_raros(dirs):
    """En Bronze los valores inválidos se conservan; rechazarlos es tarea de Silver."""
    raw_dir, bronze_dir = dirs
    (raw_dir / "pagos.csv").write_text(
        "pago_id,cliente_id,fecha_pago,monto,estado\n"
        "P001,C001,31/02/2026,abc,EXITOSO\n"  # fecha imposible y monto no numérico
        "P002,,2026-09-05,-10,RARO\n",  # id vacío, monto negativo, estado raro
        encoding="utf-8",
    )
    conteos = ingestar_todo(raw_dir, bronze_dir)
    assert conteos["pagos"] == 2


# --------------------------------------------------------------------------
# Manifiesto (linaje)
# --------------------------------------------------------------------------
def test_manifiesto_se_escribe(dirs):
    raw_dir, bronze_dir = dirs
    ingestar_todo(raw_dir, bronze_dir)

    ruta = bronze_dir / "_manifest.json"
    assert ruta.exists(), "No se creó _manifest.json (¿olvidaste escribir el JSON?)"

    manifiesto = json.loads(ruta.read_text(encoding="utf-8"))
    assert {m["tabla"] for m in manifiesto} == set(TABLAS_ESPERADAS)
    for m in manifiesto:
        assert m["filas"] == FILAS_ESPERADAS[m["tabla"]]
        assert {"archivo_origen", "sha256", "ingestado_en"} <= m.keys()


def test_hash_del_manifiesto_corresponde_al_archivo(dirs):
    raw_dir, bronze_dir = dirs
    ingestar_todo(raw_dir, bronze_dir)
    manifiesto = json.loads((bronze_dir / "_manifest.json").read_text(encoding="utf-8"))
    for m in manifiesto:
        assert m["sha256"] == calcular_sha256(bronze_dir / f"{m['tabla']}.csv")


def test_hash_cambia_si_cambia_un_byte(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("id\n1\n", encoding="utf-8")
    b.write_text("id\n2\n", encoding="utf-8")
    assert calcular_sha256(a) != calcular_sha256(b)


# --------------------------------------------------------------------------
# Idempotencia
# --------------------------------------------------------------------------
def test_ejecutar_dos_veces_da_el_mismo_resultado(dirs):
    raw_dir, bronze_dir = dirs

    conteos_1 = ingestar_todo(raw_dir, bronze_dir)
    m1 = json.loads((bronze_dir / "_manifest.json").read_text(encoding="utf-8"))

    conteos_2 = ingestar_todo(raw_dir, bronze_dir)
    m2 = json.loads((bronze_dir / "_manifest.json").read_text(encoding="utf-8"))

    assert conteos_1 == conteos_2
    assert [m["sha256"] for m in m1] == [m["sha256"] for m in m2]
    # Ojo: "ingestado_en" SÍ cambia entre corridas, y eso es correcto.


# --------------------------------------------------------------------------
# Errores: el pipeline debe fallar ruidosamente y temprano
# --------------------------------------------------------------------------
@pytest.mark.parametrize("tabla_faltante", list(TABLAS_ESPERADAS))
def test_falla_si_falta_un_archivo(dirs, tabla_faltante):
    raw_dir, bronze_dir = dirs
    (raw_dir / f"{tabla_faltante}.csv").unlink()

    with pytest.raises(FileNotFoundError, match=f"{tabla_faltante}.csv"):
        ingestar_todo(raw_dir, bronze_dir)


def test_falla_si_cambia_una_columna(dirs):
    raw_dir, bronze_dir = dirs
    (raw_dir / "pagos.csv").write_text(
        "pago_id,cliente_id,fecha_pago,importe,estado\n"  # 'monto' -> 'importe'
        "P001,C001,2026-09-05,50,EXITOSO\n",
        encoding="utf-8",
    )
    # El mensaje debe mostrar lo recibido para que el error se diagnostique solo.
    with pytest.raises(ValueError, match="importe"):
        ingestar_todo(raw_dir, bronze_dir)


def test_falla_si_falta_una_columna(dirs):
    raw_dir, bronze_dir = dirs
    (raw_dir / "clientes.csv").write_text(
        "cliente_id,estado\nC001,ACTIVO\n", encoding="utf-8"
    )
    with pytest.raises(ValueError):
        ingestar_todo(raw_dir, bronze_dir)


def test_si_la_validacion_falla_no_se_escribe_nada_en_bronze(dirs):
    """Validar ANTES de escribir: una Bronze a medias es peor que ninguna."""
    raw_dir, bronze_dir = dirs
    (raw_dir / "contactos.csv").unlink()

    with pytest.raises(FileNotFoundError):
        ingestar_todo(raw_dir, bronze_dir)

    assert not bronze_dir.exists() or not any(bronze_dir.iterdir())
