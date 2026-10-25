"""
publicar.py — Publicación del resultado del pipeline en el Data Lake (ADLS Gen2).

Se ejecuta SOLO después de que todos los lotes terminaron en SUCCESS
(Write-Audit-Publish: primero COMMIT local, después publicación).

Estructura en el contenedor (versión vigente: se sobrescribe en cada ejecución):

    bronze/clientes/clientes.csv          bronze/pagos/<batch_id>.csv  (uno por lote)
    bronze/servicios/servicios.csv        silver/<tabla>/<tabla>.parquet
    bronze/contactos/contactos.csv        gold/customer360/customer360.parquet
    _manifiestos/ejecucion_<id>.json      (historia: uno por publicación)
    _manifiestos/ultimo.json              (marcador de completitud: SIEMPRE lo último)

Si el manifiesto de una ejecución existe, todos los archivos que lista ya están
en Azure y fueron verificados por SHA-256. Si la publicación se corta a la mitad,
el manifiesto no se sube y la publicación se reconoce como incompleta.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from src import azure_storage as az

TABLAS_CARGA_COMPLETA = ["clientes", "servicios", "contactos"]
TABLAS_SILVER = TABLAS_CARGA_COMPLETA + ["pagos"]


class PublicacionError(Exception):
    """No se puede publicar: faltan archivos locales o el estado no es publicable."""


# ==========================================================================
# 1. Preparar lo que se va a subir
# ==========================================================================

def exportar_silver(db_path: Path, destino: Path) -> dict:
    """Exporta las tablas silver_* de la base OFICIAL a Parquet.

    Desde la Semana 4, Silver vive en customer360.duckdb. NO se usa data/silver/:
    ahí puede quedar un pagos.parquet antiguo (Semana 3) que no coincide con Gold.
    """
    destino = Path(destino)
    destino.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rutas = {}
        for tabla in TABLAS_SILVER:
            ruta = destino / f"{tabla}.parquet"
            con.execute(
                f"COPY (SELECT * FROM silver_{tabla} ORDER BY 1) "
                f"TO '{ruta.as_posix()}' (FORMAT PARQUET)"
            )
            rutas[tabla] = ruta
    finally:
        con.close()
    return rutas


def archivos_a_publicar(base_dir: Path, silver_exportado: dict) -> list:
    """Lista de (ruta_local, ruta_remota). Verifica que TODO exista ANTES de subir nada."""
    base = Path(base_dir)
    pares = [(base / "bronze" / f"{t}.csv", f"bronze/{t}/{t}.csv") for t in TABLAS_CARGA_COMPLETA]

    lotes = sorted((base / "bronze" / "pagos").glob("*.csv"))
    if not lotes:
        raise PublicacionError("No hay lotes de pagos en bronze/pagos/: no hay nada que publicar")
    pares += [(lote, f"bronze/pagos/{lote.name}") for lote in lotes]

    pares += [(ruta, f"silver/{t}/{t}.parquet") for t, ruta in silver_exportado.items()]
    pares.append((base / "gold" / "customer360.parquet", "gold/customer360/customer360.parquet"))

    faltantes = [str(local) for local, _ in pares if not local.exists()]
    if faltantes:
        raise PublicacionError(f"Faltan archivos locales; no se publica nada: {faltantes}")
    return pares


# ==========================================================================
# 2. El manifiesto 
# ==========================================================================

CAMPOS_LOTE = [
    "batch_id", "run_id",
    "registros_entrada", "registros_insertados", "registros_actualizados",
    "registros_ignorados", "registros_rechazados",
]


def construir_manifiesto(ejecucion_id: str, fecha_corte: str,
                         resultados_lotes: list, archivos: list) -> dict:
    """Arma el manifiesto de la publicación.

    Recibe:
      resultados_lotes  lo que devolvió procesar_lote por cada lote, en orden
                        (dicts con batch_id, run_id, registros_*, y "gold")
      archivos          lo que devolvió subir_y_verificar por cada archivo
                        (dicts con ruta_remota, sha256, bytes)

    Debe devolver:
      {
        "ejecucion_id":      ejecucion_id,
        "fecha_publicacion": fecha y hora actual en UTC, formato ISO 8601,
        "fecha_corte":       fecha_corte,
        "lotes":   [ por cada lote, SOLO los CAMPOS_LOTE ],
        "gold":    el dict "gold" del ÚLTIMO lote (filas y elegibles finales),
        "archivos": archivos ordenados por ruta_remota,
        "totales": {"archivos": <cantidad>, "bytes": <suma de bytes>},
      }
    """
    archivos_ordenados = sorted(archivos, key=lambda a: a["ruta_remota"])

    return {
        "ejecucion_id": ejecucion_id,
        "fecha_publicacion": datetime.now(timezone.utc).isoformat(),
        "fecha_corte": fecha_corte,
        "lotes": [
            {campo: r[campo] for campo in CAMPOS_LOTE}
            for r in resultados_lotes
        ],
        "gold": resultados_lotes[-1]["gold"],
        "archivos": archivos_ordenados,
        "totales": {
            "archivos": len(archivos_ordenados),
            "bytes": sum(a["bytes"] for a in archivos_ordenados),
        },
    }


# ==========================================================================
# 3. Publicar
# ==========================================================================

def publicar(contenedor_cliente, base_dir: Path, ejecucion_id: str, fecha_corte: str,
             resultados_lotes: list, db_path: Path = None, prefijo_raiz: str = "") -> dict:
    """Sube Bronze, Silver y Gold verificando cada archivo, y al FINAL el manifiesto.

    `prefijo_raiz` permite publicar bajo una carpeta aislada (lo usa la prueba de
    integración para no tocar los datos "oficiales" del contenedor).
    """
    base = Path(base_dir)
    db_path = Path(db_path) if db_path else base / "customer360.duckdb"
    preparacion = base / "publicacion" / ejecucion_id
    raiz = prefijo_raiz.strip("/")

    def remota(ruta: str) -> str:
        return f"{raiz}/{ruta}" if raiz else ruta

    # Todo lo local se prepara y valida ANTES de la primera subida.
    silver = exportar_silver(db_path, preparacion / "silver")
    pares = archivos_a_publicar(base, silver)

    publicados = [
        az.subir_y_verificar(contenedor_cliente, local, remota(destino), preparacion / "verificacion")
        for local, destino in pares
    ]

    manifiesto = construir_manifiesto(ejecucion_id, fecha_corte, resultados_lotes, publicados)
    ruta_manifiesto = preparacion / "manifiesto.json"
    ruta_manifiesto.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")

    # Marcador de completitud: solo llega aquí si TODOS los archivos se verificaron.
    az.subir_y_verificar(contenedor_cliente, ruta_manifiesto,
                         remota(f"_manifiestos/ejecucion_{ejecucion_id}.json"), preparacion / "verificacion")
    az.subir_y_verificar(contenedor_cliente, ruta_manifiesto,
                         remota("_manifiestos/ultimo.json"), preparacion / "verificacion")
    return manifiesto