-- ===========================================================================
-- Estado persistente del pipeline  (data/customer360.duckdb)
--
-- La base DuckDB es la FUENTE OFICIAL. Los Parquet/CSV de data/gold/ son
-- copias derivadas que se regeneran después de cada COMMIT.
--
-- Este script es idempotente (IF NOT EXISTS): se ejecuta en cada corrida.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- Silver incremental: UNA fila por pago_id = la versión vigente.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_pagos (
    pago_id             VARCHAR       PRIMARY KEY,
    cliente_id          VARCHAR       NOT NULL,
    fecha_pago          DATE          NOT NULL,
    monto               DECIMAL(12,2) NOT NULL,
    estado              VARCHAR       NOT NULL,
    fecha_actualizacion TIMESTAMP     NOT NULL,   -- criterio de precedencia entre versiones

    -- Linaje: qué lote escribió ESTA versión. Es metadato, NO dato de negocio:
    -- nunca se usa para decidir si un registro "cambió" (rompería la idempotencia).
    batch_id            VARCHAR       NOT NULL
);

-- Rechazados por reglas de calidad, por lote. Se REEMPLAZAN por batch_id en
-- cada aplicación (DELETE + INSERT): si se agregaran, reejecutar un lote
-- duplicaría los rechazados y el estado ya no sería idéntico.
CREATE TABLE IF NOT EXISTS silver_pagos_rechazados (
    batch_id            VARCHAR NOT NULL,
    pago_id             VARCHAR,
    cliente_id          VARCHAR,
    fecha_pago          VARCHAR,          -- texto original: aquí está lo que llegó mal
    monto               VARCHAR,
    estado              VARCHAR,
    fecha_actualizacion VARCHAR,
    motivo_rechazo      VARCHAR NOT NULL
);


-- ---------------------------------------------------------------------------
-- Control de lotes: protege contra reutilizar un batch_id con otro contenido.
-- Reaplicar el MISMO lote (mismo hash) está permitido: debe ser idempotente.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control_lotes (
    batch_id        VARCHAR   PRIMARY KEY,
    archivo         VARCHAR   NOT NULL,
    sha256          VARCHAR   NOT NULL,
    registrado_en   TIMESTAMP NOT NULL
);


-- ---------------------------------------------------------------------------
-- Auditoría: una fila por EJECUCIÓN (no por lote). Es historia: se acumula.
-- Las filas SUCCESS se insertan DENTRO de la transacción de datos.
-- Las filas FAILED se insertan DESPUÉS del ROLLBACK, en otra transacción,
-- para que el rollback no borre la evidencia del fallo.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auditoria_ejecuciones (
    run_id                  VARCHAR   PRIMARY KEY,
    batch_id                VARCHAR   NOT NULL,
    fecha_inicio            TIMESTAMP NOT NULL,
    fecha_fin               TIMESTAMP NOT NULL,
    estado                  VARCHAR   NOT NULL CHECK (estado IN ('SUCCESS', 'FAILED')),
    registros_entrada       INTEGER,
    registros_insertados    INTEGER,
    registros_actualizados  INTEGER,
    registros_ignorados     INTEGER,
    registros_rechazados    INTEGER,          -- extra: hace cuadrar la reconciliación
    mensaje_error           VARCHAR
);


-- ---------------------------------------------------------------------------
-- Cargas completas (igual que Semana 3) y Gold.
-- Se recrean con CREATE OR REPLACE TABLE dentro de la transacción de cada
-- ejecución, por eso no se declaran aquí:
--   silver_clientes, silver_servicios, silver_contactos, gold_customer360
-- ---------------------------------------------------------------------------