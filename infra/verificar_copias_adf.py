"""
    $env:AZURE_STORAGE_ACCOUNT = "<cuenta>"
    python -m infra.verificar_copias_adf <origen> <destino> [<origen> <destino> ...]

Ejemplo:
    python -m infra.verificar_copias_adf `
        gold/customer360/customer360.parquet _adf/procesados/run_001/customer360.parquet
"""

import logging
import sys
import tempfile
from pathlib import Path

from src import azure_storage as az


def sha256_remoto(fs, ruta: str, carpeta: Path) -> str:
    copia = az.descargar_archivo(fs, ruta, carpeta / Path(ruta).name)
    try:
        return az.sha256_de_bytes(copia.read_bytes())
    finally:
        copia.unlink(missing_ok=True)


def version(fs, ruta: str) -> tuple[str, str]:
    """(fecha de última modificación, etag): el etag cambia con CADA escritura."""
    try:
        p = fs.get_file_client(ruta).get_file_properties()
    except Exception as e:  # noqa: BLE001 - se traduce a un mensaje legible
        raise az.traducir_error(e, f"Propiedades de {ruta}") from e
    return p.last_modified.strftime("%Y-%m-%d %H:%M:%S UTC"), p.etag.strip('"')


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or len(argv) % 2:
        print(__doc__, file=sys.stderr)
        return 2
    logging.getLogger("azure").setLevel(logging.ERROR)

    try:
        fs = az.obtener_contenedor()
        hay_diferencias = False
        with tempfile.TemporaryDirectory() as carpeta:
            tmp = Path(carpeta)
            for origen, destino in zip(argv[::2], argv[1::2]):
                sha_o = sha256_remoto(fs, origen, tmp)
                sha_d = sha256_remoto(fs, destino, tmp)
                modificado, etag = version(fs, destino)
                igual = sha_o == sha_d
                hay_diferencias |= not igual
                print(f"{'IGUAL    ' if igual else 'DISTINTO '} {destino}")
                print(f"          origen  {sha_o}  ({origen})")
                print(f"          destino {sha_d}")
                print(f"          modificado {modificado}   etag {etag}\n")
    except az.AccesoAzureError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 1 if hay_diferencias else 0


if __name__ == "__main__":
    sys.exit(main())