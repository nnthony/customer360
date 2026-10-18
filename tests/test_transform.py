"""
Tests de la capa Silver (src/transform.py).

Tus datos reales están limpios, así que NUNCA activan las reglas de rechazo.
Aquí fabricamos datos sucios a propósito: cada regla debe verse fallar
al menos una vez para confiar en ella.

Ejecutar desde la raíz:  python -m pytest -v
"""

from pathlib import Path

import duckdb
import pytest

from src.transform import ORDEN_TABLAS, transformar_todo

# --------------------------------------------------------------------------
# Datos base: 100% válidos. Cada test reemplaza SOLO la tabla que le interesa.
# --------------------------------------------------------------------------
ENCABEZADOS = {
    "clientes": "cliente_id,fecha_alta,estado",
    "servicios": "servicio_id,cliente_id,tipo_servicio,estado,cargo_mensual",
    "pagos": "pago_id,cliente_id,fecha_pago,monto,estado",
    "contactos": "contacto_id,cliente_id,fecha_contacto,canal",
}
FILAS_VALIDAS = {
    "clientes": ["C001,2025-01-10,ACTIVO", "C002,2025-02-15,INACTIVO"],
    "servicios": ["S001,C001,INTERNET,ACTIVO,100"],
    "pagos": ["P001,C001,2026-09-05,50,EXITOSO"],
    "contactos": ["T001,C001,2026-09-01,EMAIL"],
}


def escribir_csv(bronze_dir: Path, tabla: str, filas: list[str]) -> None:
    contenido = ENCABEZADOS[tabla] + "\n" + "\n".join(filas) + "\n"
    (bronze_dir / f"{tabla}.csv").write_text(contenido, encoding="utf-8")


@pytest.fixture
def entorno(tmp_path: Path):
    """Crea bronze/ con datos válidos. Devuelve (bronze_dir, silver_dir)."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    bronze_dir.mkdir()
    for tabla, filas in FILAS_VALIDAS.items():
        escribir_csv(bronze_dir, tabla, filas)
    return bronze_dir, silver_dir


def leer(ruta: Path, columnas: str = "*") -> list[tuple]:
    return duckdb.sql(f"SELECT {columnas} FROM '{ruta.as_posix()}' ORDER BY 1").fetchall()


# --------------------------------------------------------------------------
# Camino feliz
# --------------------------------------------------------------------------
def test_datos_limpios_no_generan_rechazos(entorno):
    bronze_dir, silver_dir = entorno
    res = transformar_todo(bronze_dir, silver_dir)
    for tabla in ORDEN_TABLAS:
        assert res[tabla]["rechazados"] == 0, f"{tabla}: {res[tabla]['motivos']}"
        assert res[tabla]["validos"] == len(FILAS_VALIDAS[tabla])


def test_reconciliacion_cuadra_en_todas_las_tablas(entorno):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", FILAS_VALIDAS["pagos"] + ["P002,C999,2026-09-05,50,EXITOSO"])
    res = transformar_todo(bronze_dir, silver_dir)
    for tabla, r in res.items():
        assert r["entrada"] == r["validos"] + r["rechazados"], tabla


def test_silver_publica_tipos_correctos(entorno):
    bronze_dir, silver_dir = entorno
    transformar_todo(bronze_dir, silver_dir)

    def tipos(tabla):
        ruta = (silver_dir / f"{tabla}.parquet").as_posix()
        return {fila[0]: fila[1] for fila in duckdb.sql(f"DESCRIBE SELECT * FROM '{ruta}'").fetchall()}

    assert tipos("clientes")["fecha_alta"] == "DATE"
    assert tipos("pagos")["fecha_pago"] == "DATE"
    assert tipos("pagos")["monto"] == "DECIMAL(12,2)"
    assert tipos("servicios")["cargo_mensual"] == "DECIMAL(12,2)"
    assert tipos("contactos")["fecha_contacto"] == "DATE"
    assert "motivo_rechazo" not in tipos("pagos"), "Silver no debe exponer la columna técnica"


def test_normaliza_espacios_y_mayusculas(entorno):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", ["  P001 , C001 , 2026-09-05 , 50 ,  exitoso "])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["pagos"]["validos"] == 1
    assert leer(silver_dir / "pagos.parquet", "pago_id, estado") == [("P001", "EXITOSO")]


# --------------------------------------------------------------------------
# Una regla = un caso que la hace fallar. Una fila válida + una inválida.
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "fila_mala, motivo",
    [
        ("PX01,C001,2026-09-05,50,EXITOSO", "id_invalido"),
        (",C001,2026-09-05,50,EXITOSO", "id_invalido"),          # id vacío -> NULL
        ("P002,C999,2026-09-05,50,EXITOSO", "cliente_inexistente"),
        ("P002,,2026-09-05,50,EXITOSO", "cliente_inexistente"),   # cliente vacío
        ("P002,C001,31/02/2026,50,EXITOSO", "fecha_invalida"),
        ("P002,C001,2026-09-05,abc,EXITOSO", "monto_invalido"),
        ("P002,C001,2026-09-05,0,EXITOSO", "monto_invalido"),     # monto 0
        ("P002,C001,2026-09-05,-10,EXITOSO", "monto_invalido"),
        ("P002,C001,2026-09-05,50,RARO", "estado_invalido"),
        ("P002,C001,2026-09-05,50,", "estado_invalido"),          # trampa NULL NOT IN
    ],
)
def test_reglas_de_pagos(entorno, fila_mala, motivo):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", FILAS_VALIDAS["pagos"] + [fila_mala])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["pagos"]["validos"] == 1
    assert res["pagos"]["motivos"] == {motivo: 1}


@pytest.mark.parametrize(
    "fila_mala, motivo",
    [
        ("X001,C001,INTERNET,ACTIVO,100", "id_invalido"),     # regresión: validaba cliente_id
        ("S002,C999,INTERNET,ACTIVO,100", "cliente_inexistente"),
        ("S002,C001,,ACTIVO,100", "tipo_invalido"),
        ("S002,C001,TV,PENDIENTE,100", "estado_invalido"),
        ("S002,C001,TV,ACTIVO,0", "cargo_invalido"),
        ("S002,C001,TV,ACTIVO,cien", "cargo_invalido"),
    ],
)
def test_reglas_de_servicios(entorno, fila_mala, motivo):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "servicios", FILAS_VALIDAS["servicios"] + [fila_mala])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["servicios"]["motivos"] == {motivo: 1}


@pytest.mark.parametrize(
    "fila_mala, motivo",
    [
        ("T1,C001,2026-09-01,EMAIL", "id_invalido"),
        ("T002,C999,2026-09-01,EMAIL", "cliente_inexistente"),
        ("T002,C001,2026-13-01,EMAIL", "fecha_invalida"),     # mes 13
        ("T002,C001,2026-09-01,WHATSAPP", "canal_invalido"),
        ("T002,C001,2026-09-01,", "canal_invalido"),
    ],
)
def test_reglas_de_contactos(entorno, fila_mala, motivo):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "contactos", FILAS_VALIDAS["contactos"] + [fila_mala])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["contactos"]["motivos"] == {motivo: 1}


@pytest.mark.parametrize(
    "fila_mala, motivo",
    [
        ("c003,2025-01-10,ACTIVO", "id_invalido"),            # minúscula: el ID no se normaliza
        ("C003,10/01/2025,ACTIVO", "fecha_invalida"),
        ("C003,2025-01-10,SUSPENDIDO", "estado_invalido"),
    ],
)
def test_reglas_de_clientes(entorno, fila_mala, motivo):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "clientes", FILAS_VALIDAS["clientes"] + [fila_mala])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["clientes"]["motivos"] == {motivo: 1}


# --------------------------------------------------------------------------
# Deduplicación
# --------------------------------------------------------------------------
def test_duplicado_exacto_deja_una_sola_fila(entorno):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", FILAS_VALIDAS["pagos"] * 2)
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["pagos"]["validos"] == 1
    assert res["pagos"]["motivos"] == {"duplicado": 1}


def test_duplicado_con_copia_mala_conserva_la_buena(entorno):
    """Regresión del bug 2: sin `motivo_rechazo IS NOT NULL` se perdían las dos."""
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", [
        "P001,C001,2026-09-05,abc,EXITOSO",   # copia mala, va PRIMERO en el archivo
        "P001,C001,2026-09-05,50,EXITOSO",    # copia buena
    ])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["pagos"]["validos"] == 1
    assert res["pagos"]["motivos"] == {"monto_invalido": 1}
    assert leer(silver_dir / "pagos.parquet", "monto") == [(50,)]


def test_dedup_servicios_compara_cargo_como_numero(entorno):
    """Regresión del bug 2b: como texto, '90' > '150'. Debe ganar 150."""
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "servicios", [
        "S001,C001,INTERNET,ACTIVO,90",
        "S001,C001,INTERNET,ACTIVO,150",
    ])
    transformar_todo(bronze_dir, silver_dir)
    assert leer(silver_dir / "servicios.parquet", "cargo_mensual") == [(150,)]


# --------------------------------------------------------------------------
# Integridad referencial y rechazados
# --------------------------------------------------------------------------
def test_hijas_validan_contra_clientes_limpios(entorno):
    """Si un cliente es rechazado en Silver, sus pagos también deben caer."""
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "clientes", ["C001,2025-01-10,ACTIVO", "C002,2025-02-15,RARO"])
    escribir_csv(bronze_dir, "pagos", [
        "P001,C001,2026-09-05,50,EXITOSO",
        "P002,C002,2026-09-05,50,EXITOSO",
    ])
    res = transformar_todo(bronze_dir, silver_dir)
    assert res["clientes"]["motivos"] == {"estado_invalido": 1}
    assert res["pagos"]["motivos"] == {"cliente_inexistente": 1}


def test_rechazados_conservan_valor_original_y_motivo(entorno):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", FILAS_VALIDAS["pagos"] + ["P002,C001,31/02/2026,50,EXITOSO"])
    transformar_todo(bronze_dir, silver_dir)
    filas = leer(silver_dir / "rechazados" / "pagos.parquet", "pago_id, fecha_pago, motivo_rechazo")
    assert filas == [("P002", "31/02/2026", "fecha_invalida")]


# --------------------------------------------------------------------------
# Idempotencia
# --------------------------------------------------------------------------
def test_ejecutar_dos_veces_da_el_mismo_silver(entorno):
    bronze_dir, silver_dir = entorno
    escribir_csv(bronze_dir, "pagos", FILAS_VALIDAS["pagos"] + ["P002,C999,2026-09-05,50,EXITOSO"])

    res_1 = transformar_todo(bronze_dir, silver_dir)
    datos_1 = {t: leer(silver_dir / f"{t}.parquet") for t in ORDEN_TABLAS}

    res_2 = transformar_todo(bronze_dir, silver_dir)
    datos_2 = {t: leer(silver_dir / f"{t}.parquet") for t in ORDEN_TABLAS}

    assert res_1 == res_2
    assert datos_1 == datos_2