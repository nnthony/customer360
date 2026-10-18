-- ===========================================================================
-- Customer 360 — Audiencia objetivo
-- Grano: UNA fila por cliente (todos los clientes de Silver, aunque no tengan
--        servicios, pagos ni contactos).
--
-- Entradas (las crea pipeline.py, aquí NO van rutas de archivos):
--   clientes, servicios, pagos, contactos  -> vistas sobre Silver (ya tipadas)
--   parametros(fecha_corte DATE)           -> una sola fila
--
-- Convención de ventanas (documentar en README):
--   últimos N días = fecha > fecha_corte - N  AND  fecha <= fecha_corte
--   (N días exactos contando el día de corte; nada posterior al corte)
--
-- Elegibilidad:
--   cliente.estado = 'ACTIVO' AND servicios_vencidos = 0 AND contactos_30d < 2
-- ===========================================================================

WITH params AS (
    SELECT fecha_corte FROM parametros
),

servicios_agg AS (
    -- TODO 1: por cliente_id:
    --   cantidad_servicios_activos, cargo_mensual_total (solo ACTIVO), servicios_vencidos
    --   Pista: COUNT(*) FILTER (WHERE estado = 'ACTIVO')
  SELECT cliente_id,
         COUNT(*) FILTER (WHERE estado = 'ACTIVO')               AS cantidad_servicios_activos,
         COALESCE(SUM(cargo_mensual) FILTER (WHERE estado = 'ACTIVO'), 0) AS cargo_mensual_total,
         COUNT(*) FILTER (WHERE estado = 'VENCIDO')              AS servicios_vencidos
  FROM servicios
  GROUP BY cliente_id
),

pagos_agg AS (
    -- TODO 2: por cliente_id, SOLO pagos EXITOSO y con fecha_pago <= fecha_corte:
    --   ultimo_pago_exitoso = MAX(fecha_pago)
    --   monto_pagado_90d    = SUM(monto) dentro de la ventana de 90 días
    --   Pista: FROM pagos CROSS JOIN params  (como tu INNER JOIN ... ON 1=1 de Oracle)
  SELECT cliente_id,
         MAX(fecha_pago) AS ultimo_pago_exitoso,
         SUM(monto) FILTER (WHERE fecha_pago > fecha_corte - 90) AS monto_pagado_90d
  FROM pagos CROSS JOIN params
  WHERE estado = 'EXITOSO' AND fecha_pago <= fecha_corte
  GROUP BY cliente_id
),

contactos_agg AS (
    -- TODO 3: por cliente_id, contactos_30d dentro de la ventana de 30 días
  SELECT cliente_id, COUNT(*) AS contactos_30d
  FROM contactos CROSS JOIN params
  WHERE fecha_contacto >  fecha_corte - 30
    AND fecha_contacto <= fecha_corte
  GROUP BY cliente_id
)

-- TODO 4: SELECT final
--   * Partir de `clientes` (no de servicios: si no, pierdes clientes sin servicios)
--   * LEFT JOIN a cada *_agg  (ya agregados -> sin fan-out)
--   * COALESCE(..., 0) en todo lo numérico; ultimo_pago_exitoso puede quedar NULL
--   * es_elegible calculado AQUÍ, sobre valores ya con COALESCE (adiós bug del NULL)
--   * Columnas EN ESTE ORDEN: cliente_id, cantidad_servicios_activos, cargo_mensual_total,
--     ultimo_pago_exitoso, monto_pagado_90d, contactos_30d, es_elegible
--   * ORDER BY cliente_id
SELECT c.cliente_id,
       COALESCE(s.cantidad_servicios_activos, 0) AS cantidad_servicios_activos,
       COALESCE(s.cargo_mensual_total, 0)        AS cargo_mensual_total,
       p.ultimo_pago_exitoso,
       COALESCE(p.monto_pagado_90d, 0)           AS monto_pagado_90d,
       COALESCE(k.contactos_30d, 0)              AS contactos_30d,
       (c.estado = 'ACTIVO'
        AND COALESCE(s.servicios_vencidos, 0) = 0
        AND COALESCE(k.contactos_30d, 0) < 2)    AS es_elegible
FROM clientes c
LEFT JOIN servicios_agg s USING (cliente_id)
LEFT JOIN pagos_agg     p USING (cliente_id)
LEFT JOIN contactos_agg k USING (cliente_id)
ORDER BY c.cliente_id;
