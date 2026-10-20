"""
transform.py — Capa Silver (Bronze -> Silver)

Responsabilidad: aplicar reglas de calidad documentadas y producir datos
TIPADOS y CONFIABLES en Parquet.

Patrón para cada tabla (Write-Audit-Publish simplificado):
  1. Leer Bronze como texto y normalizar (TRIM, UPPER).
  2. Evaluar reglas -> cada fila recibe un `motivo_rechazo` (NULL = válida).
  3. Deduplicar por clave primaria.
  4. Publicar:  válidos    -> silver/<tabla>.parquet           (tipados)
                rechazados -> silver/rechazados/<tabla>.parquet (texto + motivo)
  5. Reconciliar: filas_bronze == válidos + rechazados. Si no cuadra, FALLA.

Regla de oro: NUNCA se borra una fila en silencio. Toda fila que entra
termina en válidos o en rechazados, con su motivo.
"""

from pathlib import Path

import duckdb

from src.ingest import contar_filas

# Orden importa: clientes primero, porque las demás tablas validan
# que su cliente_id exista en clientes (integridad referencial).
ORDEN_TABLAS = ["clientes", "servicios", "pagos", "contactos"]

# Contrato de salida de Silver: qué columnas y con qué TIPO se publican.
# Los CAST aquí son seguros porque solo se aplican a filas que ya pasaron TRY_CAST.
COLUMNAS_SILVER = {
    "clientes": """
        cliente_id,
        CAST(fecha_alta AS DATE) AS fecha_alta,
        estado
    """,
    "servicios": """
        servicio_id,
        cliente_id,
        tipo_servicio,
        estado,
        CAST(cargo_mensual AS DECIMAL(12, 2)) AS cargo_mensual
    """,
    "pagos": """
        pago_id,
        cliente_id,
        CAST(fecha_pago AS DATE) AS fecha_pago,
        CAST(monto AS DECIMAL(12, 2)) AS monto,
        estado
    """,
    "contactos": """
        contacto_id,
        cliente_id,
        CAST(fecha_contacto AS DATE) AS fecha_contacto,
        canal
    """,
}


def sql_clientes(ruta_bronze: str) -> str:
    """EJEMPLO RESUELTO. Úsalo como plantilla para las otras tablas."""
    return f"""
    WITH base AS (
        SELECT
            TRIM(cliente_id)    AS cliente_id,
            TRIM(fecha_alta)    AS fecha_alta,
            UPPER(TRIM(estado)) AS estado
        FROM read_csv('{ruta_bronze}', all_varchar = true)
    ),
    reglas AS (
        SELECT *,
            -- Un solo motivo por fila: el PRIMERO que falle, en este orden.
            CASE
                WHEN cliente_id IS NULL
                  OR NOT regexp_matches(cliente_id, '^C[0-9]{{3}}$') THEN 'id_invalido'
                WHEN TRY_CAST(fecha_alta AS DATE) IS NULL            THEN 'fecha_invalida'
                -- ¡Ojo! `NULL NOT IN (...)` da NULL, no TRUE. Por eso el IS NULL explícito.
                WHEN estado IS NULL
                  OR estado NOT IN ('ACTIVO', 'INACTIVO')            THEN 'estado_invalido'
            END AS motivo_rechazo
        FROM base
    ),
    dedup AS (
        SELECT *,
            -- Si un id se repite, se queda la fila VÁLIDA (FALSE ordena antes que TRUE)
            -- y, entre válidas, la de fecha_alta más antigua. El resto = 'duplicado'.
            ROW_NUMBER() OVER (
                PARTITION BY cliente_id
                ORDER BY motivo_rechazo IS NOT NULL, fecha_alta
            ) AS rn
        FROM reglas
    )
    SELECT
        * EXCLUDE (rn, motivo_rechazo),
        COALESCE(motivo_rechazo, CASE WHEN rn > 1 THEN 'duplicado' END) AS motivo_rechazo
    FROM dedup
    """


def sql_servicios(ruta_bronze: str) -> str:
    return f"""
    WITH base AS (
        SELECT
            TRIM(servicio_id)          AS servicio_id,
            TRIM(cliente_id)           AS cliente_id,
            UPPER(TRIM(tipo_servicio)) AS tipo_servicio,
            UPPER(TRIM(estado))        AS estado,
            TRIM(cargo_mensual)        AS cargo_mensual
        FROM read_csv('{ruta_bronze}', all_varchar = true)
    ),
    reglas AS (
        SELECT *,
            CASE
                WHEN servicio_id IS NULL
                  OR NOT regexp_matches(servicio_id, '^S[0-9]{{3}}$') THEN 'id_invalido'
                -- Integridad referencial (ya resuelta): el cliente debe existir en Silver.
                WHEN cliente_id IS NULL
                  OR cliente_id NOT IN (SELECT cliente_id FROM clientes_silver) THEN 'cliente_inexistente'
                WHEN tipo_servicio IS NULL THEN 'tipo_invalido'
                WHEN estado IS NULL
                  OR estado NOT IN ('ACTIVO', 'VENCIDO','CANCELADO')            THEN 'estado_invalido'
                WHEN TRY_CAST(cargo_mensual AS DECIMAL(12, 2)) IS NULL OR TRY_CAST(cargo_mensual AS DECIMAL(12, 2)) <= 0 THEN 'cargo_invalido'
            END AS motivo_rechazo
        FROM base
    ),
    dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY servicio_id
                ORDER BY
                    motivo_rechazo IS NOT NULL,                          -- 1º: la válida gana
                    TRY_CAST(cargo_mensual AS DECIMAL(12, 2)) DESC,      -- 2º: mayor cargo
                    estado      
            ) AS rn
        FROM reglas
    )
    SELECT
        * EXCLUDE (rn, motivo_rechazo),
        COALESCE(motivo_rechazo, CASE WHEN rn > 1 THEN 'duplicado' END) AS motivo_rechazo
    FROM dedup
    """


def sql_pagos(ruta_bronze: str) -> str:
    return f"""
    WITH base AS (
        SELECT
            TRIM(pago_id) AS pago_id,
            TRIM(cliente_id) AS cliente_id,
            TRIM(fecha_pago) AS fecha_pago,
            TRIM(monto) AS monto,
            UPPER(TRIM(estado)) AS estado
        FROM read_csv('{ruta_bronze}', all_varchar = true)
    ),
    reglas AS (
        SELECT *,
            CASE
                WHEN pago_id IS NULL
                  OR NOT regexp_matches(pago_id, '^P[0-9]{{3}}$') THEN 'id_invalido'
                WHEN cliente_id IS NULL
                  OR cliente_id NOT IN (SELECT cliente_id FROM clientes_silver) THEN 'cliente_inexistente'
                WHEN TRY_CAST(fecha_pago AS DATE) IS NULL THEN 'fecha_invalida'
                WHEN TRY_CAST(monto AS DECIMAL(12, 2)) IS NULL OR TRY_CAST(monto AS DECIMAL(12, 2)) <= 0 THEN 'monto_invalido'
                WHEN estado IS NULL
                  OR estado NOT IN ('EXITOSO', 'FALLIDO')   THEN 'estado_invalido'
            END AS motivo_rechazo
        FROM base
    ),
    dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pago_id
            ORDER BY
                motivo_rechazo IS NOT NULL,
                TRY_CAST(fecha_pago AS DATE),
                TRY_CAST(monto AS DECIMAL(12, 2)) DESC
            ) AS rn
        FROM reglas
    )
    SELECT
        * EXCLUDE (rn, motivo_rechazo),
        COALESCE(motivo_rechazo, CASE WHEN rn > 1 THEN 'duplicado' END) AS motivo_rechazo
    FROM dedup
    """


def sql_contactos(ruta_bronze: str) -> str:
    return f"""
    WITH base AS (
        SELECT
            TRIM(contacto_id) AS contacto_id,
            TRIM(cliente_id) AS cliente_id,
            TRIM(fecha_contacto) AS fecha_contacto,
            UPPER(TRIM(canal)) AS canal
        FROM read_csv('{ruta_bronze}', all_varchar = true)
    ),
    reglas AS (
        SELECT *,
            CASE
                WHEN contacto_id IS NULL
                  OR NOT regexp_matches(contacto_id, '^T[0-9]{{3}}$') THEN 'id_invalido'
                WHEN cliente_id IS NULL
                  OR cliente_id NOT IN (SELECT cliente_id FROM clientes_silver) THEN 'cliente_inexistente'
                WHEN TRY_CAST(fecha_contacto AS DATE) IS NULL THEN 'fecha_invalida'
                WHEN canal IS NULL
                  OR canal NOT IN ('EMAIL', 'LLAMADA','CHAT')   THEN 'canal_invalido'
            END AS motivo_rechazo
        FROM base
    ),
    dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY contacto_id
            ORDER BY
                motivo_rechazo IS NOT NULL,
                TRY_CAST(fecha_contacto AS DATE)
            ) AS rn
        FROM reglas
    )
    SELECT
        * EXCLUDE (rn, motivo_rechazo),
        COALESCE(motivo_rechazo, CASE WHEN rn > 1 THEN 'duplicado' END) AS motivo_rechazo
    FROM dedup
    """


REGLAS = {
    "clientes": sql_clientes,
    "servicios": sql_servicios,
    "pagos": sql_pagos,
    "contactos": sql_contactos,
}


def procesar_tabla(con, tabla: str, bronze_dir: Path, silver_dir: Path) -> dict:
    """Valida una tabla de Bronze y publica válidos/rechazados en Silver."""
    ruta_bronze = bronze_dir / f"{tabla}.csv"
    ruta_validos = (silver_dir / f"{tabla}.parquet").as_posix()
    ruta_rechazados = (silver_dir / "rechazados" / f"{tabla}.parquet").as_posix()

    # 1-3. Reglas + dedup -> tabla temporal en memoria
    sql = REGLAS[tabla](ruta_bronze.as_posix())
    con.execute(f"CREATE OR REPLACE TEMP TABLE {tabla}_validado AS {sql}")

    # 4. Publicar. ORDER BY 1 = salida determinística (mismo input -> mismo archivo).
    con.execute(f"""
        COPY (
            SELECT {COLUMNAS_SILVER[tabla]}
            FROM {tabla}_validado
            WHERE motivo_rechazo IS NULL
            ORDER BY 1
        ) TO '{ruta_validos}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT * FROM {tabla}_validado
            WHERE motivo_rechazo IS NOT NULL
            ORDER BY 1
        ) TO '{ruta_rechazados}' (FORMAT PARQUET)
    """)

    # 5. Conteos y reconciliación
    filas_bronze = contar_filas(ruta_bronze)
    validos = con.execute(f"SELECT COUNT(*) FROM '{ruta_validos}'").fetchone()[0]
    motivos = dict(
        con.execute(f"""
        SELECT motivo_rechazo, COUNT(*) FROM {tabla}_validado
        WHERE motivo_rechazo IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    )
    rechazados = sum(motivos.values())

    if filas_bronze != validos + rechazados:
        raise RuntimeError(
            f"{tabla}: no cuadra la reconciliación. "
            f"bronze={filas_bronze}, validos={validos}, rechazados={rechazados}"
        )

    return {
        "entrada": filas_bronze,
        "validos": validos,
        "rechazados": rechazados,
        "motivos": motivos,
    }


def transformar_todo(bronze_dir: Path, silver_dir: Path, tablas=None) -> dict:
    tablas = tablas or ORDEN_TABLAS
    bronze_dir, silver_dir = Path(bronze_dir), Path(silver_dir)
    (silver_dir / "rechazados").mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()  # base en memoria: no deja archivos .duckdb
    resultado = {}
    try:
        for tabla in tablas:
            resultado[tabla] = procesar_tabla(con, tabla, bronze_dir, silver_dir)

            if tabla == "clientes":
                # Las tablas hijas validan contra los clientes YA LIMPIOS.
                ruta = (silver_dir / "clientes.parquet").as_posix()
                con.execute(
                    f"CREATE OR REPLACE TEMP VIEW clientes_silver AS "
                    f"SELECT cliente_id FROM '{ruta}'"
                )
    finally:
        con.close()
    return resultado


if __name__ == "__main__":
    #   python -m src.transform
    res = transformar_todo(Path("data/bronze"), Path("data/silver"))
    for tabla, r in res.items():
        print(
            f"[silver] {tabla:<10} entrada={r['entrada']:>3}  "
            f"validos={r['validos']:>3}  rechazados={r['rechazados']:>3}  {r['motivos']}"
        )
