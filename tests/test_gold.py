"""
Tests de la capa Gold (sql/customer360.sql + construir_gold).

Dos tipos de test:
  1. Unitarios con datos mínimos: cada uno prueba UNA regla de negocio.
  2. Integración con tus datos reales (data/raw) y resultado esperado
     calculado a mano: el "golden test".

Fecha de corte usada: 2026-09-30 (último día con datos).
Si tu profe usó otra, cambia FECHA_CORTE y recalcula ESPERADO.
"""

import shutil
from datetime import date
from pathlib import Path

import duckdb
import pytest

from src.pipeline import RAIZ_PROYECTO, construir_gold, run
from src.transform import transformar_todo

FECHA_CORTE = "2026-09-30"

ENCABEZADOS = {
    "clientes": "cliente_id,fecha_alta,estado",
    "servicios": "servicio_id,cliente_id,tipo_servicio,estado,cargo_mensual",
    "pagos": "pago_id,cliente_id,fecha_pago,monto,estado",
    "contactos": "contacto_id,cliente_id,fecha_contacto,canal",
}
# Cliente "ancla" C099: garantiza que ninguna tabla quede vacía.
# Los tests se enfocan en C001.
ANCLA = {
    "clientes": ["C099,2025-01-01,ACTIVO"],
    "servicios": ["S099,C099,TV,ACTIVO,10"],
    "pagos": ["P099,C099,2026-09-01,10,EXITOSO"],
    "contactos": ["T099,C099,2026-09-01,EMAIL"],
}


def correr(tmp_path: Path, fecha=FECHA_CORTE, **filas) -> dict:
    """Escribe Bronze, corre Silver + Gold y devuelve {cliente_id: {columna: valor}}."""
    bronze, silver, gold = tmp_path / "bronze", tmp_path / "silver", tmp_path / "gold"
    bronze.mkdir(exist_ok=True)
    for tabla, encabezado in ENCABEZADOS.items():
        lineas = [encabezado] + ANCLA[tabla] + filas.get(tabla, [])
        (bronze / f"{tabla}.csv").write_text("\n".join(lineas) + "\n", encoding="utf-8")

    transformar_todo(bronze, silver)
    construir_gold(silver, gold, fecha)

    rel = duckdb.sql(f"SELECT * FROM '{(gold / 'customer360.parquet').as_posix()}'")
    return {fila[0]: dict(zip(rel.columns, fila)) for fila in rel.fetchall()}


# --------------------------------------------------------------------------
# Grano: una fila por cliente
# --------------------------------------------------------------------------
def test_cliente_sin_actividad_aparece_con_ceros(tmp_path):
    g = correr(tmp_path, clientes=["C001,2025-01-10,ACTIVO"])
    c = g["C001"]
    assert c["cantidad_servicios_activos"] == 0
    assert c["cargo_mensual_total"] == 0
    assert c["ultimo_pago_exitoso"] is None
    assert c["monto_pagado_90d"] == 0
    assert c["contactos_30d"] == 0


def test_no_hay_fan_out(tmp_path):
    """3 servicios x 2 pagos x 2 contactos: un JOIN directo multiplicaría los totales."""
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        servicios=[
            "S001,C001,TV,ACTIVO,10",
            "S002,C001,TV,ACTIVO,20",
            "S003,C001,TV,ACTIVO,30",
        ],
        pagos=["P001,C001,2026-09-10,100,EXITOSO", "P002,C001,2026-09-20,100,EXITOSO"],
        contactos=["T001,C001,2026-09-10,EMAIL", "T002,C001,2026-09-20,CHAT"],
    )
    c = g["C001"]
    assert c["cantidad_servicios_activos"] == 3
    assert c["cargo_mensual_total"] == 60
    assert c["monto_pagado_90d"] == 200
    assert c["contactos_30d"] == 2
    assert len(g) == 2  # C001 + ancla


# --------------------------------------------------------------------------
# Servicios
# --------------------------------------------------------------------------
def test_solo_servicios_activos_suman_cargo(tmp_path):
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        servicios=[
            "S001,C001,TV,ACTIVO,50",
            "S002,C001,TV,CANCELADO,40",
            "S003,C001,TV,VENCIDO,30",
        ],
    )
    assert g["C001"]["cantidad_servicios_activos"] == 1
    assert g["C001"]["cargo_mensual_total"] == 50


# --------------------------------------------------------------------------
# Pagos y ventana de 90 días  (corte 2026-09-30 -> ventana 2026-07-03 .. 2026-09-30)
# --------------------------------------------------------------------------
def test_ventana_90_dias_y_ultimo_pago(tmp_path):
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        pagos=[
            "P001,C001,2026-07-02,10,EXITOSO",  # día 91 -> fuera
            "P002,C001,2026-07-03,20,EXITOSO",  # día 90 -> dentro
            "P003,C001,2026-09-30,30,EXITOSO",  # día de corte -> dentro
            "P004,C001,2026-10-01,40,EXITOSO",  # posterior al corte -> fuera
            "P005,C001,2026-09-29,999,FALLIDO",  # fallido -> nunca cuenta
        ],
    )
    assert g["C001"]["monto_pagado_90d"] == 50
    assert g["C001"]["ultimo_pago_exitoso"] == date(2026, 9, 30)


def test_ultimo_pago_exitoso_puede_ser_antiguo(tmp_path):
    """El último pago exitoso NO está limitado a 90 días."""
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        pagos=["P001,C001,2026-01-15,10,EXITOSO"],
    )
    assert g["C001"]["ultimo_pago_exitoso"] == date(2026, 1, 15)
    assert g["C001"]["monto_pagado_90d"] == 0


# --------------------------------------------------------------------------
# Contactos y ventana de 30 días  (ventana 2026-09-01 .. 2026-09-30)
# --------------------------------------------------------------------------
def test_ventana_30_dias(tmp_path):
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        contactos=[
            "T001,C001,2026-08-31,EMAIL",  # día 31 -> fuera
            "T002,C001,2026-09-01,EMAIL",  # día 30 -> dentro
            "T003,C001,2026-09-30,EMAIL",  # día de corte -> dentro
            "T004,C001,2026-10-01,EMAIL",  # futuro -> fuera
        ],
    )
    assert g["C001"]["contactos_30d"] == 2


# --------------------------------------------------------------------------
# Elegibilidad
# --------------------------------------------------------------------------
def test_cliente_sin_contactos_es_elegible(tmp_path):
    """Regresión del bug de tu consulta Oracle: NULL < 2 no es TRUE."""
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        servicios=["S001,C001,TV,ACTIVO,50"],
    )
    assert g["C001"]["contactos_30d"] == 0
    assert g["C001"]["es_elegible"] is True


def test_cliente_inactivo_no_es_elegible(tmp_path):
    """Regresión del caso C009: inactivo con servicio activo."""
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,INACTIVO"],
        servicios=["S001,C001,TV,ACTIVO,50"],
    )
    assert g["C001"]["es_elegible"] is False


def test_servicio_vencido_quita_elegibilidad(tmp_path):
    g = correr(
        tmp_path,
        clientes=["C001,2025-01-10,ACTIVO"],
        servicios=["S001,C001,TV,ACTIVO,50", "S002,C001,TV,VENCIDO,30"],
    )
    assert g["C001"]["es_elegible"] is False


@pytest.mark.parametrize(
    "n_contactos, elegible", [(0, True), (1, True), (2, False), (3, False)]
)
def test_limite_de_contactos(tmp_path, n_contactos, elegible):
    contactos = [f"T00{i},C001,2026-09-1{i},EMAIL" for i in range(1, n_contactos + 1)]
    g = correr(tmp_path, clientes=["C001,2025-01-10,ACTIVO"], contactos=contactos)
    assert g["C001"]["es_elegible"] is elegible


# --------------------------------------------------------------------------
# Parámetro y contrato
# --------------------------------------------------------------------------
def test_la_fecha_de_corte_cambia_el_resultado(tmp_path):
    datos = dict(
        clientes=["C001,2025-01-10,ACTIVO"],
        contactos=["T001,C001,2026-09-10,EMAIL", "T002,C001,2026-09-20,EMAIL"],
    )
    assert correr(tmp_path, "2026-09-30", **datos)["C001"]["es_elegible"] is False
    assert correr(tmp_path, "2026-10-25", **datos)["C001"]["es_elegible"] is True


def test_fecha_de_corte_invalida_falla(tmp_path):
    with pytest.raises(ValueError):
        correr(tmp_path, "30/09/2026")


def test_verificacion_detecta_clientes_duplicados(tmp_path):
    """Un SQL que duplica filas NO debe publicarse."""
    correr(tmp_path)  # genera silver válido
    sql_malo = tmp_path / "malo.sql"
    sql_malo.write_text(
        """
        SELECT cliente_id, 0 AS cantidad_servicios_activos, 0 AS cargo_mensual_total,
               NULL::DATE AS ultimo_pago_exitoso, 0 AS monto_pagado_90d,
               0 AS contactos_30d, TRUE AS es_elegible
        FROM clientes, (VALUES (1), (2)) t(x)
    """,
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="una fila por cliente"):
        construir_gold(
            tmp_path / "silver", tmp_path / "gold_malo", FECHA_CORTE, sql_malo
        )
    assert not (tmp_path / "gold_malo" / "customer360.parquet").exists()


# --------------------------------------------------------------------------
# Golden test: tus 4 CSV reales, resultado calculado a mano
# --------------------------------------------------------------------------
# Decisión documentada: "Activo" = estado del CLIENTE. Por eso C001
# (activo, sin servicios) es elegible. Si decides exigir >= 1 servicio activo,
# C001 pasa a False: cambia el SQL, este valor y el README.
ESPERADO = {
    #        serv  cargo  ultimo_pago          m90  c30  elegible
    "C001": (0, 0, date(2026, 9, 5), 50, 0, True),
    "C002": (3, 200, date(2026, 9, 10), 300, 1, True),
    "C003": (1, 90, date(2026, 9, 15), 90, 1, False),  # servicio vencido
    "C004": (1, 120, None, 0, 0, True),  # sin pagos
    "C005": (1, 80, date(2026, 9, 20), 160, 2, False),  # 2 contactos
    "C006": (2, 145, date(2026, 9, 1), 290, 0, True),  # 07-03 entra, 07-02 no
    "C007": (1, 70, date(2026, 9, 5), 140, 1, True),  # 08-31 no entra
    "C008": (2, 155, date(2026, 9, 30), 310, 2, False),  # 2 contactos
    "C009": (1, 90, date(2026, 9, 12), 180, 1, False),  # cliente INACTIVO
    "C010": (4, 245, date(2026, 9, 25), 980, 4, False),  # 4 contactos
}


def test_golden_datos_reales(tmp_path):
    raw_real = RAIZ_PROYECTO / "data" / "raw"
    if not (raw_real / "clientes.csv").exists():
        pytest.skip("No hay datos en data/raw")
    shutil.copytree(raw_real, tmp_path / "raw")

    log = run(tmp_path, FECHA_CORTE)
    assert log["gold"]["filas"] == 10
    assert log["gold"]["elegibles"] == 5

    rel = duckdb.sql(
        f"SELECT * FROM '{(tmp_path / 'gold' / 'customer360.parquet').as_posix()}'"
    )
    obtenido = {fila[0]: tuple(fila[1:]) for fila in rel.fetchall()}
    for cliente_id, esperado in ESPERADO.items():
        assert obtenido[cliente_id] == esperado, cliente_id
