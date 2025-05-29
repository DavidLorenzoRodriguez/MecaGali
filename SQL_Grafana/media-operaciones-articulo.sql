SELECT ROUND(AVG(num_operaciones), 2) AS media_operaciones_por_articulo
FROM (
    SELECT CODIGO_INTERNO, COUNT(DISTINCT DESC_OPERACION) AS num_operaciones
    FROM operaciones_relacionadas
    WHERE DESC_OPERACION IS NOT NULL AND DESC_OPERACION != 'Sin Operaciones'
    GROUP BY CODIGO_INTERNO
) AS subconsulta;
