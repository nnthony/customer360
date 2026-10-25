"""
probar_permisos.py — Muestra qué puede hacer la identidad ACTUAL en el Data Lake.

Usa la misma cadena de DefaultAzureCredential que el pipeline:
  - si AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET están definidas,
    actúa como ese service principal (EnvironmentCredential);
  - si no, actúa como tu usuario de `az login`.

No contiene secretos: los lee del entorno de la terminal.

    python -m infra.probar_permisos
"""

import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

from src import azure_storage as az


def _escribir(fs, tmp: Path):
    archivo = tmp / "prueba.txt"
    archivo.write_text("prueba de permisos", encoding="utf-8")
    destino = f"gold/_prueba_permisos_{uuid.uuid4().hex[:6]}.txt"
    az.subir_archivo(fs, archivo, destino)
    fs.delete_file(destino)          # si pudo escribir, no deja basura


PRUEBAS = [
    ("Listar   gold/", lambda fs, tmp: az.listar_archivos(fs, "gold")),
    ("Leer     gold/customer360/customer360.parquet",
     lambda fs, tmp: az.descargar_archivo(fs, "gold/customer360/customer360.parquet", tmp / "g.parquet")),
    ("Listar   bronze/", lambda fs, tmp: az.listar_archivos(fs, "bronze")),
    ("Leer     bronze/clientes/clientes.csv",
     lambda fs, tmp: az.descargar_archivo(fs, "bronze/clientes/clientes.csv", tmp / "c.csv")),
    ("Escribir gold/", _escribir),
]


def main() -> int:
    logging.getLogger("azure").setLevel(logging.ERROR)
    cliente_id = os.environ.get("AZURE_CLIENT_ID")
    identidad = f"service principal {cliente_id}" if cliente_id else "tu usuario (az login)"
    print(f"Identidad: {identidad}\n")

    try:
        fs = az.obtener_contenedor()
    except az.AccesoAzureError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as carpeta:
        tmp = Path(carpeta)
        for nombre, accion in PRUEBAS:
            try:
                accion(fs, tmp)
                resultado = "OK"
            except az.AccesoAzureError as e:
                resultado = "DENEGADO (403)" if "403" in str(e) else f"ERROR: {e}"
            print(f"  {nombre:<48} {resultado}")
    return 0


if __name__ == "__main__":
    sys.exit(main())