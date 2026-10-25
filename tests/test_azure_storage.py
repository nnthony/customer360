"""
Pruebas UNITARIAS de src/azure_storage.py — sin Azure, sin red, sin credenciales.

Se usa un contenedor FALSO en memoria (ContenedorFalso) que imita los pocos
métodos del SDK que usa el módulo. Así estas pruebas corren en cualquier lado,
incluido GitHub Actions, que no tiene (ni debe tener) acceso a Azure.

La prueba REAL contra Azure va aparte (marcador `azure`), en el Día 5.
"""

import hashlib
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)

from src import azure_storage as az
from src.ingest import calcular_sha256
from src.pipeline import RAIZ_PROYECTO


# ==========================================================================
# Contenedor falso en memoria
# ==========================================================================

class ArchivoFalso:
    def __init__(self, almacen: dict, ruta: str, corromper: bool = False):
        self.almacen, self.ruta, self.corromper = almacen, ruta, corromper

    def upload_data(self, datos, overwrite=False, metadata=None, **kwargs):
        if hasattr(datos, "read"):
            datos = datos.read()
        if self.ruta in self.almacen and not overwrite:
            raise ResourceExistsError(f"{self.ruta} ya existe")
        self.almacen[self.ruta] = {"datos": bytes(datos), "metadata": metadata or {}}

    def download_file(self, **kwargs):
        if self.ruta not in self.almacen:
            raise ResourceNotFoundError(f"{self.ruta} no existe")
        datos = self.almacen[self.ruta]["datos"]
        if self.corromper:                       # simula una corrupción en tránsito
            datos = datos + b"X"
        return SimpleNamespace(readall=lambda: datos)


class ContenedorFalso:
    def __init__(self, corromper_descargas: bool = False):
        self.almacen: dict = {}
        self.corromper_descargas = corromper_descargas

    def get_file_client(self, ruta: str):
        return ArchivoFalso(self.almacen, ruta, self.corromper_descargas)

    def get_paths(self, path=None, recursive=True, **kwargs):
        prefijo = (path or "").rstrip("/")
        coincidencias = [r for r in sorted(self.almacen) if not prefijo or r.startswith(prefijo + "/")]

        def generador():                         # como el SDK: el error aparece AL RECORRER
            if prefijo and not coincidencias:
                raise ResourceNotFoundError(f"{prefijo} no existe")
            for r in coincidencias:
                yield SimpleNamespace(name=r, is_directory=False)
        return generador()


def error_http(estado: int) -> HttpResponseError:
    e = HttpResponseError(message="AuthorizationPermissionMismatch")
    e.status_code = estado
    return e


@pytest.fixture
def archivo(tmp_path: Path) -> Path:
    ruta = tmp_path / "clientes.csv"
    ruta.write_bytes(b"cliente_id,fecha_alta,estado\nC001,2025-01-10,ACTIVO\n")
    return ruta


# ==========================================================================
# Operaciones con archivos (tu Día 4)
# ==========================================================================

def test_sha256_coincide_con_el_de_bronze(archivo):
    """El hash de Azure y el del manifiesto de Bronze deben ser comparables."""
    assert az.sha256_de_bytes(archivo.read_bytes()) == calcular_sha256(archivo)


def test_subir_devuelve_hash_y_guarda_el_contenido(archivo):
    fs = ContenedorFalso()
    h = az.subir_archivo(fs, archivo, "bronze/clientes/clientes.csv")

    assert h == hashlib.sha256(archivo.read_bytes()).hexdigest()
    assert fs.almacen["bronze/clientes/clientes.csv"]["datos"] == archivo.read_bytes()


def test_subir_dos_veces_sobrescribe(archivo):
    """Idempotencia: reejecutar la publicación no debe fallar."""
    fs = ContenedorFalso()
    az.subir_archivo(fs, archivo, "bronze/clientes/clientes.csv")
    az.subir_archivo(fs, archivo, "bronze/clientes/clientes.csv")
    assert len(fs.almacen) == 1


def test_listar_solo_lo_que_esta_bajo_el_prefijo(archivo):
    fs = ContenedorFalso()
    for ruta in ("bronze/clientes/a.csv", "bronze/pagos/b.csv", "silver/clientes/c.parquet"):
        az.subir_archivo(fs, archivo, ruta)

    assert az.listar_archivos(fs, "bronze") == ["bronze/clientes/a.csv", "bronze/pagos/b.csv"]
    assert az.listar_archivos(fs, "silver") == ["silver/clientes/c.parquet"]


def test_listar_ruta_inexistente_devuelve_lista_vacia():
    assert az.listar_archivos(ContenedorFalso(), "gold") == []


def test_descargar_crea_carpetas_y_conserva_bytes(archivo, tmp_path):
    fs = ContenedorFalso()
    az.subir_archivo(fs, archivo, "bronze/clientes/clientes.csv")

    destino = az.descargar_archivo(fs, "bronze/clientes/clientes.csv", tmp_path / "a" / "b" / "x.csv")
    assert destino.read_bytes() == archivo.read_bytes()


def test_subir_y_verificar_conserva_el_sha256(archivo, tmp_path):
    """Prueba obligatoria 1 (versión unitaria): ida y vuelta con el mismo SHA-256."""
    r = az.subir_y_verificar(ContenedorFalso(), archivo, "bronze/clientes/clientes.csv", tmp_path / "tmp")
    assert r["sha256"] == calcular_sha256(archivo)
    assert r["bytes"] == archivo.stat().st_size
    assert r["ruta_remota"] == "bronze/clientes/clientes.csv"


def test_subir_y_verificar_detecta_corrupcion(archivo, tmp_path):
    """Si lo descargado difiere de lo enviado, NO se da por bueno."""
    fs = ContenedorFalso(corromper_descargas=True)
    with pytest.raises(az.IntegridadError):
        az.subir_y_verificar(fs, archivo, "bronze/clientes/clientes.csv", tmp_path / "tmp")


# ==========================================================================
# Prueba obligatoria 4: error de acceso (simulado)
# ==========================================================================

def test_acceso_denegado_se_traduce_a_mensaje_util(archivo):
    fs = MagicMock()
    fs.get_file_client.return_value.upload_data.side_effect = error_http(403)

    with pytest.raises(az.AccesoAzureError, match="403") as exc:
        az.subir_archivo(fs, archivo, "bronze/clientes/clientes.csv")
    assert "Storage Blob Data Contributor" in str(exc.value)


def test_sin_identidad_falla_rapido_con_un_solo_intento():
    credencial = MagicMock()
    credencial.get_token.side_effect = ClientAuthenticationError("sin sesión")

    with pytest.raises(az.AccesoAzureError, match="az login"):
        az.obtener_contenedor("cuenta", "customer360", credencial=credencial)
    credencial.get_token.assert_called_once()


# ==========================================================================
# Prueba obligatoria 3: autenticación por identidad, sin claves
# ==========================================================================

def test_usa_identidad_de_entra_id_y_endpoint_dfs():
    with patch.object(az, "DefaultAzureCredential") as dac, \
         patch.object(az, "DataLakeServiceClient") as servicio:
        az.obtener_contenedor("micuenta", "customer360")

    servicio.assert_called_once_with(
        account_url="https://micuenta.dfs.core.windows.net",
        credential=dac.return_value,
    )


def test_no_hay_secretos_en_el_codigo():
    """Busca patrones de cadenas de conexión, claves de cuenta y tokens SAS."""
    patrones = [r"AccountKey\s*=", r"SharedAccessSignature", r"[?&]sig=", r"DefaultEndpointsProtocol"]
    archivos = list((RAIZ_PROYECTO / "src").glob("*.py")) + list((RAIZ_PROYECTO / "infra").glob("*"))
    for ruta in archivos:
        texto = ruta.read_text(encoding="utf-8", errors="ignore")
        for patron in patrones:
            assert not re.search(patron, texto), f"Posible secreto en {ruta.name}: {patron}"