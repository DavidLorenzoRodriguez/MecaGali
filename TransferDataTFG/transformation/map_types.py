def map_sqlserver_to_mysql(datatype: str, length):
    mapping = {
        "int": "INT",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "bit": "BOOLEAN",
        "varchar": lambda l: f"VARCHAR({int(l)})" if l and int(l) > 0 else "TEXT",
        "nvarchar": lambda l: f"VARCHAR({int(l)})" if l and int(l) > 0 else "TEXT",
        "datetime": "DATETIME",
        "date": "DATE",
        "float": "FLOAT",
        "decimal": "DECIMAL(10, 2)",
        "char": lambda l: f"CHAR({int(l)})" if l and int(l) > 0 else "TEXT",
        "varbinary": "LONGBLOB",  # para imágenes o datos binarios
        "image": "LONGBLOB",      # QL Server usa image para datos binarios
    }

    if datatype in ["varchar", "nvarchar", "char"]:
        if not length or int(length) <= 0:
            return "TEXT"
        return mapping[datatype](length)

    return mapping.get(datatype, "TEXT")
