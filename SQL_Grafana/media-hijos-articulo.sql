SELECT ROUND(AVG(cantidad_hijos), 2) AS media_hijos_por_articulo
FROM (
    SELECT VER_ARTICULO_PADRE, COUNT(DISTINCT VER_ARTICULO_HIJO) AS cantidad_hijos
    FROM jerarquia_articulos
    WHERE VER_ARTICULO_PADRE IS NOT NULL
    GROUP BY VER_ARTICULO_PADRE
) AS subconsulta;
