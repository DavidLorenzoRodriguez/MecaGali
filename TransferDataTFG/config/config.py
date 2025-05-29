# Configuración de conexiones para SQL Server y MySQL
SQL_SERVER_ORIGEN_CONFIG = {
    "server": "Elastic.mrfs.lan",
    "database": "elastic_mrf",
    "username": "mrf",
    "password": "mrf1234",
    "port": 42019
}


MYSQL_CONFIG = {
    "host": "10.32.1.65",
    "database": "mecagal",
    "user": "david.lorenzo@outlook.es",
    "password": "2StFvscrUWu95HUL"
}


PRIMARY_KEYS_CONFIG = {
    'vsales_customers_active': ['customer_id'],
    'ALM_VISTA_STOCK_ARTICULOS': ['ID_ARTICULO'],
    'vmrf_prod_order_details': ['ID_ORDEN_DETALLE'],
    'vmrf_alm_maestro_articulos': ['ID_ARTICULO'],
    'vmrf_signing_control_workers' : ['employee_hour_id'],
    'PROD_ORDENES_DETALLES' : ['ID_ORDEN_DETALLE'],
    'PROD_VISTA_ARTICULOS' : ['ID_VERSION_ARTICULO'],
    'PROD_VISTA_COMPOSICIONES' : ['ID_COMPOSICION'],
    'vmrf_prod_operaciones_despiece': ['ID_OPERACION_ARTICULO'],
    'purchases_suppliers' : ['supplier_id'],
    'purchases_orders' : ['order_id'],
    'vsales_customers' : ['customer_id'],
    'vsales_customers_total' : ['customer_id'],
    'sales_invoice' : ['invoice_id'],
    'vsales_customers_items': ['item_id', 'customer_id'],
 

    # Añade aquí todas las tablas y su PK
}
