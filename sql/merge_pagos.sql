MERGE INTO silver_pagos AS e
USING (
    SELECT
        c.pago_id,
        c.cliente_id,
        c.fecha_pago,
        c.monto,
        c.estado,
        c.fecha_actualizacion,
        p.batch_id
    FROM lote_clasificado c
    CROSS JOIN parametros_lote p
    -- WHERE c.accion IN ('INSERT', 'UPDATE')
) AS s
ON e.pago_id = s.pago_id


WHEN MATCHED THEN UPDATE SET
    cliente_id=s.cliente_id,
    fecha_pago=s.fecha_pago,
    monto=s.monto,
    estado=s.estado,
    fecha_actualizacion = s.fecha_actualizacion,
    batch_id = s.batch_id

WHEN NOT MATCHED THEN INSERT (batch_id, pago_id, cliente_id, fecha_pago, monto, estado, fecha_actualizacion)
    VALUES (s.batch_id, s.pago_id, s.cliente_id, s.fecha_pago, s.monto, s.estado, s.fecha_actualizacion)
;
