SELECT 
  CODIGO_PADRE,
  DESC_ARTICULO,
  ROUND(TIEMPO_TOTAL, 2) AS TIEMPO_TOTAL_MINUTOS
FROM tiempo_fabricacion_resumen
ORDER BY TIEMPO_TOTAL DESC
LIMIT 1
