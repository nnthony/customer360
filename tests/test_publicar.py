"""
Pruebas UNITARIAS de la publicación (src/publicar.py) con el ContenedorFalso:
sin Azure, corren en CI.

Prueba obligatoria 2 (versión unitaria): los archivos quedan en las rutas
Bronze, Silver y Gold esperadas.
"""

import hashlib
import shutil
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from src import azure_storage as az
from src.pipeline import RAIZ_PROYECTO
from src.pipeline_incremental import main, procesar_lote
from src.publicar import PublicacionError, publicar
from tests.test_azure_storage import ContenedorFalso

RAW_REAL = RAIZ_PROYECTO / "data" / "raw"
LOTES = [RAW_REAL / "pagos" / f"pagos_{n}.csv" for n in ("inicial", "lote_001", "lote_002")]
CONFLICTO = RAIZ_PROYECTO / "tests" / "fixtures" / "pagos_lote_conflicto.csv"
FECHA = "2026-09-30"


@pytest.fixture
def base(tmp_path: Path) -> Path:
    (tmp_path / "raw").mkdir()
    for tabla in ("clientes", "servicios", "contactos"):
        shutil.copy(RAW_REAL / f"{tabla}.csv", tmp_path / "raw" / f"{tabla}.csv")
    return tmp_path


@pytest.fixture
def ejecutado(base):
    """Pipeline local completo (3 lotes OK). Devuelve (base, resultados)."""
    resultados = [procesar_lote(base, lote, FECHA) for lote in LOTES]
    return base, resultados


def publicar_en_falso(base, resultados, fs=None):
    fs = fs or ContenedorFalso()
    manifiesto = publicar(fs, base, "ejec-001", FECHA, resultados)
    return fs, manifiesto


# --------------------------------------------------------------------------
def test_publica_en_las_rutas_esperadas(ejecutado):
    base, resultados = ejecutado
    fs, _ = publicar_en_falso(base, resultados)

    assert az.listar_archivos(fs, "bronze") == [
        "bronze/clientes/clientes.csv",
        "bronze/contactos/contactos.csv",
        "bronze/pagos/inicial.csv",
        "bronze/pagos/lote_001.csv",
        "bronze/pagos/lote_002.csv",
        "bronze/servicios/servicios.csv",
    ]
    assert az.listar_archivos(fs, "silver") == [
        "silver/clientes/clientes.parquet",
        "silver/contactos/contactos.parquet",
        "silver/pagos/pagos.parquet",
        "silver/servicios/servicios.parquet",
    ]
    assert az.listar_archivos(fs, "gold") == ["gold/customer360/customer360.parquet"]


def test_el_manifiesto_es_lo_ultimo_que_se_sube(ejecutado):
    """Marcador de completitud: si existe, todo lo demás ya está arriba."""
    base, resultados = ejecutado
    fs, _ = publicar_en_falso(base, resultados)
    orden = list(fs.almacen)
    assert orden[-2:] == ["_manifiestos/ejecucion_ejec-001.json", "_manifiestos/ultimo.json"]


def test_manifiesto_describe_lotes_y_archivos(ejecutado):
    base, resultados = ejecutado
    fs, m = publicar_en_falso(base, resultados)

    assert m["ejecucion_id"] == "ejec-001" and m["fecha_corte"] == FECHA
    assert "fecha_publicacion" in m
    assert [lote["batch_id"] for lote in m["lotes"]] == ["inicial", "lote_001", "lote_002"]
    assert m["lotes"][1]["registros_entrada"] == 8
    assert "gold" not in m["lotes"][0], "los lotes llevan solo sus conteos"
    assert m["gold"] == {"filas": 10, "elegibles": 5}
    assert m["totales"]["archivos"] == len(m["archivos"]) == 11
    assert m["totales"]["bytes"] == sum(a["bytes"] for a in m["archivos"])

    # Cada hash del manifiesto corresponde a lo que REALMENTE quedó en el contenedor
    for a in m["archivos"]:
        assert a["sha256"] == hashlib.sha256(fs.almacen[a["ruta_remota"]]["datos"]).hexdigest()


def test_silver_se_publica_desde_la_base_y_no_desde_archivos_viejos(ejecutado, tmp_path):
    """Un pagos.parquet antiguo en data/silver/ (resto de la Semana 3) NO debe subirse."""
    base, resultados = ejecutado
    (base / "silver" / "pagos.parquet").write_bytes(b"archivo viejo de la semana 3")

    fs, _ = publicar_en_falso(base, resultados)
    copia = tmp_path / "pagos.parquet"
    copia.write_bytes(fs.almacen["silver/pagos/pagos.parquet"]["datos"])
    assert duckdb.sql(f"SELECT COUNT(*) FROM '{copia.as_posix()}'").fetchone()[0] == 33


def test_si_un_archivo_no_se_verifica_no_se_sube_el_manifiesto(ejecutado):
    base, resultados = ejecutado
    fs = ContenedorFalso(corromper_descargas=True)
    with pytest.raises(az.IntegridadError):
        publicar_en_falso(base, resultados, fs)
    assert not any(r.startswith("_manifiestos/") for r in fs.almacen)


def test_si_falta_gold_no_se_sube_nada(ejecutado):
    base, resultados = ejecutado
    (base / "gold" / "customer360.parquet").unlink()
    fs = ContenedorFalso()
    with pytest.raises(PublicacionError, match="customer360.parquet"):
        publicar_en_falso(base, resultados, fs)
    assert fs.almacen == {}


# --------------------------------------------------------------------------
# Línea de comandos: --publicar
# --------------------------------------------------------------------------
def test_cli_publica_tras_una_ejecucion_exitosa(base):
    fs = ContenedorFalso()
    with patch.object(az, "obtener_contenedor", return_value=fs):
        codigo = main(["--lote", *map(str, LOTES), "--fecha-corte", FECHA,
                       "--base-dir", str(base), "--publicar"])
    assert codigo == 0
    assert "_manifiestos/ultimo.json" in fs.almacen


def test_cli_no_publica_si_falla_un_lote(base):
    """No se sube Gold de una ejecución fallida: ni siquiera se conecta a Azure."""
    fs = ContenedorFalso()
    with patch.object(az, "obtener_contenedor", return_value=fs) as conectar:
        codigo = main(["--lote", str(LOTES[0]), str(CONFLICTO), "--fecha-corte", FECHA,
                       "--base-dir", str(base), "--publicar"])
    assert codigo == 1
    conectar.assert_not_called()
    assert fs.almacen == {}