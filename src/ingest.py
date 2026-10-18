"""
ingest.py — Capa Bronze (Raw -> Bronze)

Responsabilidad: preservar los datos de entrada TAL CUAL llegaron,
y dejar evidencia de qué se ingirió (conteos + huella digital).

NO hace: casteos, filtros, deduplicación ni reglas de negocio.
Eso es trabajo de transform.py (Silver).
"""

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Contrato de entrada: qué archivos esperamos y con qué columnas.
# Si mañana el sistema origen cambia una columna, el pipeline debe
# FALLAR aquí, al inicio, y no tres pasos después con un error raro.
TABLAS_ESPERADAS = {
    "clientes": ["cliente_id", "fecha_alta", "estado"],
    "servicios": [
        "servicio_id",
        "cliente_id",
        "tipo_servicio",
        "estado",
        "cargo_mensual",
    ],
    "pagos": ["pago_id", "cliente_id", "fecha_pago", "monto", "estado"],
    "contactos": ["contacto_id", "cliente_id", "fecha_contacto", "canal"],
}


def validar_archivos_entrada(raw_dir: Path) -> None:
    """Verifica que existan los 4 CSV y que tengan exactamente las columnas esperadas.

    Lanza FileNotFoundError si falta un archivo.
    Lanza ValueError si las columnas no coinciden.
    """
    for tabla, columnas_esperadas in TABLAS_ESPERADAS.items():
        ruta = raw_dir / f"{tabla}.csv"

        if not ruta.exists():
            raise FileNotFoundError(
                f"Falta el archivo '{tabla}.csv' en {raw_dir.resolve()}"
            )

        columnas_reales = duckdb.sql(
            f"SELECT * FROM read_csv('{ruta}', all_varchar=true) LIMIT 0"
        ).columns

        if columnas_reales != columnas_esperadas:
            raise ValueError(
                f"Columnas inválidas en {tabla}.csv\n"
                f"  esperadas: {columnas_esperadas}\n"
                f"  recibidas: {columnas_reales}"
            )


def contar_filas(ruta_csv: Path) -> int:
    """Devuelve el número de filas de datos (sin contar el encabezado)."""
    numero_filas = duckdb.sql(
        f"SELECT COUNT(*) FROM read_csv('{ruta_csv}', all_varchar=true)"
    ).fetchone()[0]
    return numero_filas


def calcular_sha256(ruta: Path) -> str:
    """Huella digital del archivo: si cambia un solo byte, cambia el hash."""
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def ingestar_tabla(tabla: str, raw_dir: Path, bronze_dir: Path) -> dict:
    """Copia un CSV de raw a bronze y devuelve sus metadatos de ingesta."""
    origen = raw_dir / f"{tabla}.csv"
    destino = bronze_dir / f"{tabla}.csv"

    shutil.copy2(origen, destino)

    filas_raw = contar_filas(origen)
    filas_bronze = contar_filas(destino)

    if filas_raw != filas_bronze:
        raise RuntimeError(
            f"{tabla}: raw tiene {filas_raw} filas y bronze {filas_bronze}"
        )

    return {
        "tabla": tabla,
        "archivo_origen": str(origen),
        "filas": filas_bronze,
        "sha256": calcular_sha256(destino),
        "ingestado_en": datetime.now(timezone.utc).isoformat(),
    }


def ingestar_todo(raw_dir: Path, bronze_dir: Path) -> dict:
    """Ejecuta la capa Bronze completa. Devuelve {tabla: filas}."""
    raw_dir, bronze_dir = Path(raw_dir), Path(bronze_dir)

    validar_archivos_entrada(raw_dir)  # primero validar, luego escribir
    bronze_dir.mkdir(parents=True, exist_ok=True)

    manifiesto = [ingestar_tabla(t, raw_dir, bronze_dir) for t in TABLAS_ESPERADAS]

    ruta_manifiesto = bronze_dir / "_manifest.json"
    ruta_manifiesto.write_text(
        json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    return {m["tabla"]: m["filas"] for m in manifiesto}


if __name__ == "__main__":
    # Prueba manual rápida, ejecutar desde la raíz del proyecto:
    #   python -m src.ingest
    conteos = ingestar_todo(Path("data/raw"), Path("data/bronze"))
    for tabla, n in conteos.items():
        print(f"[bronze] {tabla:<10} {n:>4} filas")
