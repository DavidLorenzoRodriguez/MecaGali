# Configuración de conexiones para SQL Server y MySQL
SQL_SERVER_ORIGEN_CONFIG = {
    "server": "",
    "database": "",
    "username": "",
    "password": "",
    "port":
}


MYSQL_CONFIG = {
    "host": "",
    "database": "",
    "user": "",
    "password": ""
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
