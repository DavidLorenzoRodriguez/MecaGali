# Solución BI para MecaGali

Este proyecto consiste en el desarrollo e implementación de una plataforma de Business Intelligence (BI) para optimizar los procesos de producción de MecaGali, una empresa del sector de componentes metálicos para la automoción.

## Objetivo del Proyecto

Proporcionar una herramienta integrada para analizar datos operativos, anticipar necesidades productivas y facilitar la toma de decisiones basada en datos. El sistema extrae, transforma y visualiza información clave proveniente de ERP, CRM y sistema de fichaje.

## Arquitectura General

- **Backend**: Apache Airflow (ETL)
- **Visualización**: Grafana
- **Base de Datos**: MySQL (MariaDB)
- **Infraestructura**: Docker sobre Proxmox
- **Lenguaje principal**: Python

## Funcionalidades Principales

- Generación jerárquica de artículos (padre ↔ hijo)
- Asociación de operaciones a los artículos
- Cálculo de duración media por operación
- Transformación y análisis de datos de fichaje
- Cálculo del tiempo total de fabricación por artículo padre
- Dashboards con KPIs: eficiencia, productividad, carga de trabajo, etc.

## DAGs implementados en Airflow

| DAG | Descripción |
|-----|-------------|
| `dag_duracion_media_operacion` | Calcula duración media de cada operación desde fichajes |
| `dag_extract_transform_fichaje` | Extrae y transforma fichajes para análisis de RRHH |
| `1dag_generar_jerarquia_articulos` | Genera jerarquía de artículos de 2 niveles desde Elastic ERP |
| `2dag_despiece_operaciones` | Relaciona operaciones con artículos padres e hijos |
| `3dag_resumen_tiempo_fabricacion` | Resume el tiempo total de fabricación por artículo padre |

## Proceso ETL

Automatizado con Apache Airflow:

1. Extracción desde SQL Server y MariaDB
2. Transformación y limpieza de datos
3. Carga en una base de datos MariaDB 
4. Tablas filtradas para visualización en Grafana


## Requisitos Técnicos

- Docker + Docker Compose
- Python 3.10+
- Apache Airflow 2.6+
- Grafana
- MySQL (MariaDB)
- Conexión con ERP vía SQL Server

## Autor

**David Lorenzo Rodríguez**  
Grado en Ingeniería Informática  
Universidad Europea de Madrid, curso 2024-2025

## Licencia

Proyecto académico bajo licencia MIT. Uso profesional permitido con reconocimiento.
