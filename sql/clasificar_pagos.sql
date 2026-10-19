CREATE OR REPLACE TEMP TABLE lote_clasificado AS
WITH comparado AS (
    SELECT
        s.*,
        CASE
            WHEN e.pago_id IS NULL THEN 'nuevo'
            WHEN s.fecha_actualizacion > e.fecha_actualizacion THEN 'version_posterior'
            WHEN s.fecha_actualizacion < e.fecha_actualizacion THEN 'version_antigua'

            WHEN s.fecha_actualizacion IS NOT DISTINCT FROM e.fecha_actualizacion
                AND s.cliente_id IS NOT DISTINCT FROM e.cliente_id
                AND s.fecha_pago IS NOT DISTINCT FROM e.fecha_pago
                AND s.monto IS NOT DISTINCT FROM e.monto
                AND s.estado IS NOT DISTINCT FROM e.estado
                    THEN 'identico' 
            WHEN s.fecha_actualizacion IS NOT DISTINCT FROM e.fecha_actualizacion
                AND (s.cliente_id IS DISTINCT FROM e.cliente_id
                OR s.fecha_pago IS DISTINCT FROM e.fecha_pago
                OR s.monto IS DISTINCT FROM e.monto
                OR s.estado IS DISTINCT FROM e.estado)
                    THEN 'conflicto' 
        END AS detalle
    FROM lote_staging s
    LEFT JOIN silver_pagos e USING (pago_id)
)
SELECT
    *,
    CASE detalle
        WHEN 'nuevo'             THEN 'INSERT'
        WHEN 'version_posterior' THEN 'UPDATE'
        WHEN 'version_antigua'   THEN 'IGNORE'
        WHEN 'identico'          THEN 'IGNORE'
        WHEN 'conflicto'         THEN 'CONFLICT'
    END AS accion
FROM comparado