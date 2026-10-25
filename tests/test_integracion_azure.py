"""
Para ejecutarla:
    $env:AZURE_STORAGE_ACCOUNT = "<cuenta>"
    $env:CUSTOMER360_PRUEBAS_AZURE = "1"
    az login --tenant <tenant-id>
    python -m pytest -m azure -v

"""

import json
import os
import shutil
import uuid
from pathlib import Path

import pytest

from src import azure_storage as az
from src.ingest import calcular_sha256
from src.pipeline import RAIZ_PROYECTO
from src.pipeline_incremental import procesar_lote
from src.publicar import publicar

pytestmark = [
    pytest.mark.azure,
    pytest.mark.skipif(
        os.environ.get("CUSTOMER360_PRUEBAS_AZURE") != "1" or not os.environ.get("AZURE_STORAGE_ACCOUNT"),
        reason="Prueba contra Azure real: requiere AZURE_STORAGE_ACCOUNT, az login y CUSTOMER360_PRUEBAS_AZURE=1",
    ),
]

RAW_REAL = RAIZ_PROYECTO / "data" / "raw"
FECHA = "2026-09-30"


def test_integracion_publica_y_verifica_en_azure(tmp_path: Path):
    # Dado: una ejecución local exitosa
    (tmp_path / "raw").mkdir()
    for tabla in ("clientes", "servicios", "contactos"):
        shutil.copy(RAW_REAL / f"{tabla}.csv", tmp_path / "raw" / f"{tabla}.csv")
    resultados = [
        procesar_lote(tmp_path, RAW_REAL / "pagos" / f"pagos_{n}.csv", FECHA)
        for n in ("inicial", "lote_001", "lote_002")
    ]

    fs = az.obtener_contenedor()
    prefijo = f"_pruebas/{uuid.uuid4().hex[:8]}"
    try:
        # Cuando: se publica
        publicar(fs, tmp_path, "integracion", FECHA, resultados, prefijo_raiz=prefijo)

        # Entonces (obligatoria 2): las rutas esperadas existen en Azure
        archivos = az.listar_archivos(fs, prefijo)
        for esperado in ("bronze/clientes/clientes.csv", "bronze/pagos/lote_002.csv",
                         "silver/pagos/pagos.parquet", "gold/customer360/customer360.parquet",
                         "_manifiestos/ultimo.json"):
            assert f"{prefijo}/{esperado}" in archivos, esperado

        # Entonces (obligatoria 5): Gold en Azure es idéntico al Gold local
        gold = az.descargar_archivo(fs, f"{prefijo}/gold/customer360/customer360.parquet", tmp_path / "gold_azure.parquet")
        assert calcular_sha256(gold) == calcular_sha256(tmp_path / "gold" / "customer360.parquet")

        # Y el manifiesto descargado lista exactamente lo que hay en Azure
        manifiesto = json.loads(
            az.descargar_archivo(fs, f"{prefijo}/_manifiestos/ultimo.json", tmp_path / "m.json").read_text(encoding="utf-8")
        )
        listados = {a["ruta_remota"] for a in manifiesto["archivos"]}
        assert listados <= set(archivos)
    finally:
        fs.delete_directory(prefijo)