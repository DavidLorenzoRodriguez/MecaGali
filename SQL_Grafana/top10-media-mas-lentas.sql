SELECT 
  `DESC_OPERACION` AS "Operación", 
  ROUND(`duracion_media_minutos` , 2) AS "Duración Media (min)"
FROM 
  `duracion_media_operacion`
ORDER BY 
  `duracion_media_minutos` ASC
LIMIT 10;
