SELECT
  `DESC_OPERACION` AS "Operación",
  ROUND(`duracion_media_minutos` / 1000, 2) AS "Duración Media (min)"
FROM
  `duracion_media_operacion`
WHERE
  `DESC_OPERACION` IN (${operacion:singlequote})
ORDER BY
  `duracion_media_minutos` DESC;
