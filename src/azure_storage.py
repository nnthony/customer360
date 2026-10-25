"""
Configuración (variables de entorno):
    AZURE_STORAGE_ACCOUNT    nombre de la cuenta (obligatoria)
    AZURE_STORAGE_CONTAINER  contenedor (por defecto: customer360)

Prueba de conexión:
    python -m src.azure_storage
"""

import hashlib
import logging
import os
import sys
import uuid
from pathlib import Path

from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
)
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

CONTENEDOR_POR_DEFECTO = "customer360"
ALCANCE_STORAGE = "https://storage.azure.com/.default"


class AccesoAzureError(Exception):
    """No hay identidad válida o la identidad no tiene permisos sobre el Data Lake."""


class IntegridadError(Exception):
    """El archivo descargado no coincide (SHA-256) con el original."""


# ==========================================================================
# Configuración y cliente
# ==========================================================================

def configuracion() -> tuple[str, str]:
    """Lee cuenta y contenedor del entorno. Falla con un mensaje claro si falta algo."""
    cuenta = os.environ.get("AZURE_STORAGE_ACCOUNT")
    if not cuenta:
        raise AccesoAzureError(
            "Falta la variable de entorno AZURE_STORAGE_ACCOUNT. "
            'En PowerShell: $env:AZURE_STORAGE_ACCOUNT = "<nombre-de-la-cuenta>"'
        )
    contenedor = os.environ.get("AZURE_STORAGE_CONTAINER", CONTENEDOR_POR_DEFECTO)
    return cuenta, contenedor


def obtener_contenedor(cuenta: str = None, contenedor: str = None, credencial=None):
    """Devuelve un cliente del contenedor (FileSystemClient).

    `credencial` se puede inyectar: en producción es DefaultAzureCredential, y en
    las pruebas unitarias será un objeto simulado (mock), sin tocar Azure.
    """
    if cuenta is None or contenedor is None:
        cuenta_env, contenedor_env = configuracion()
        cuenta = cuenta or cuenta_env
        contenedor = contenedor or contenedor_env

    credencial = credencial or DefaultAzureCredential()
    comprobar_identidad(credencial)
    servicio = DataLakeServiceClient(
        account_url=f"https://{cuenta}.dfs.core.windows.net",
        credential=credencial,
    )
    return servicio.get_file_system_client(contenedor)


def comprobar_identidad(credencial) -> None:
    """Fail fast: pide UN token antes de operar.

    Sin esto, el primer fallo de autenticación ocurre dentro de una operación del
    SDK de Storage, cuya política de reintentos lo repite varias veces en vano.
    """
    try:
        credencial.get_token(ALCANCE_STORAGE)
    except ClientAuthenticationError as e:
        raise traducir_error(e, "Autenticación") from e


def traducir_error(error: Exception, operacion: str) -> AccesoAzureError:
    """Convierte errores del SDK en un mensaje que dice QUÉ hacer."""
    if isinstance(error, ClientAuthenticationError):
        return AccesoAzureError(
            f"{operacion}: no se encontró una identidad válida. "
            "Ejecuta `az login --tenant <tenant-id>` y vuelve a intentar."
        )
    if isinstance(error, HttpResponseError) and error.status_code == 403:
        return AccesoAzureError(
            f"{operacion}: acceso denegado (403). Tu identidad necesita un rol de DATOS, "
            "por ejemplo 'Storage Blob Data Contributor' sobre el contenedor. "
            "Ser Owner de la cuenta no basta."
        )
    if isinstance(error, ResourceNotFoundError):
        return AccesoAzureError(f"{operacion}: la ruta no existe en el Data Lake.")
    return AccesoAzureError(f"{operacion}: {type(error).__name__}: {error}")


# ==========================================================================
# Verificación de acceso
# ==========================================================================

def verificar_acceso(contenedor_cliente) -> dict:
    """Prueba REAL de lectura y escritura: crea, lista y borra un directorio temporal.

    Leer no basta para saber si podrás subir archivos: por eso se escribe.
    """
    prueba = f"_verificacion/{uuid.uuid4().hex[:8]}"
    try:
        contenedor_cliente.create_directory(prueba)
        nombres = [p.name for p in contenedor_cliente.get_paths(path="_verificacion")]
        contenedor_cliente.delete_directory("_verificacion")
    except (ClientAuthenticationError, HttpResponseError) as e:
        raise traducir_error(e, "Verificar acceso") from e

    if prueba not in nombres:
        raise AccesoAzureError(f"Se creó {prueba} pero no aparece al listar: {nombres}")

    return {
        "cuenta": contenedor_cliente.account_name,
        "contenedor": contenedor_cliente.file_system_name,
        "escritura": True,
        "lectura": True,
    }


# ==========================================================================
# Operaciones con archivos
# ==========================================================================

def sha256_de_bytes(datos: bytes) -> str:
    """SHA-256 en hexadecimal de un bloque de bytes (mismo valor que calcular_sha256
    de ingest.py para el mismo contenido: así los hashes de Bronze y de Azure se comparan)."""
    return hashlib.sha256(datos).hexdigest()


def subir_archivo(contenedor_cliente, ruta_local: Path, ruta_remota: str) -> str:
    """Sube un archivo local a `ruta_remota` (sobrescribiendo) y devuelve su SHA-256.

    El hash queda también como metadato del archivo remoto: así el archivo en Azure
    se describe a sí mismo, sin depender de un manifiesto aparte.
    """
    datos = Path(ruta_local).read_bytes()
    sha = sha256_de_bytes(datos)
    try:
        cliente_archivo = contenedor_cliente.get_file_client(ruta_remota)
        cliente_archivo.upload_data(datos, overwrite=True, metadata={"sha256": sha})
    except (ClientAuthenticationError, HttpResponseError) as e:
        raise traducir_error(e, f"Subir {ruta_remota}") from e
    return sha


def listar_archivos(contenedor_cliente, prefijo: str) -> list[str]:
    """Rutas de los ARCHIVOS (no directorios) bajo `prefijo`, recursivo y ordenadas.

    Si la ruta no existe devuelve []: no es un error, simplemente no hay nada.
    """
    try:
        rutas = [
            p.name
            for p in contenedor_cliente.get_paths(path=prefijo, recursive=True)
            if not p.is_directory
        ]
    except ResourceNotFoundError:
        return []
    except (ClientAuthenticationError, HttpResponseError) as e:
        raise traducir_error(e, f"Listar {prefijo}") from e
    return sorted(rutas)


def descargar_archivo(contenedor_cliente, ruta_remota: str, ruta_local: Path) -> Path:
    """Descarga `ruta_remota` a `ruta_local` (crea las carpetas si faltan)."""
    try:
        datos = contenedor_cliente.get_file_client(ruta_remota).download_file().readall()
    except (ClientAuthenticationError, HttpResponseError) as e:
        raise traducir_error(e, f"Descargar {ruta_remota}") from e

    ruta_local = Path(ruta_local)
    ruta_local.parent.mkdir(parents=True, exist_ok=True)
    ruta_local.write_bytes(datos)
    return ruta_local


def subir_y_verificar(contenedor_cliente, ruta_local: Path, ruta_remota: str,
                      carpeta_temporal: Path) -> dict:
    """Sube, vuelve a DESCARGAR y compara SHA-256. Si no coinciden: IntegridadError.

    "upload_data no lanzó error" no es lo mismo que "llegó exactamente lo que envié":
    la única prueba es traer el archivo de vuelta y comparar huellas.
    """
    ruta_local = Path(ruta_local)
    sha_local = subir_archivo(contenedor_cliente, ruta_local, ruta_remota)

    # Nombre único: dos verificaciones simultáneas no se pisan la copia temporal.
    copia = Path(carpeta_temporal) / f"{uuid.uuid4().hex}_{Path(ruta_remota).name}"
    try:
        descargar_archivo(contenedor_cliente, ruta_remota, copia)
        sha_remoto = sha256_de_bytes(copia.read_bytes())
    finally:
        copia.unlink(missing_ok=True)   # se limpia SIEMPRE, también si algo falló

    if sha_remoto != sha_local:
        raise IntegridadError(
            f"{ruta_remota}: el archivo en Azure no coincide con el local "
            f"(local {sha_local[:12]}..., remoto {sha_remoto[:12]}...)"
        )
    return {"ruta_remota": ruta_remota, "sha256": sha_local, "bytes": ruta_local.stat().st_size}


def main() -> int:
    """Borde del programa: los errores ESPERADOS se muestran en una línea."""
    logging.getLogger("azure.identity").setLevel(logging.ERROR)
    try:
        r = verificar_acceso(obtener_contenedor())
    except AccesoAzureError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    print(f"Acceso OK a {r['cuenta']}/{r['contenedor']} (lectura y escritura) con identidad de Entra ID")
    return 0


if __name__ == "__main__":
    sys.exit(main())