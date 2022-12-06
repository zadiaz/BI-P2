from utils.file_util import cargar_datos

# city insertion
def insert_query_municipios(**kwargs):
    insert = f"INSERT INTO municipios (id,municipio,departamento) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.id},\'{row.municipio}\',\'{row.departamento}\') "
        duplicate = f"ON CONFLICT (id) DO UPDATE SET municipio=excluded.municipio, departamento=excluded.departamento;\n"
        insertQuery += duplicate
    return insertQuery

# customer insertion
def insert_query_recursos(**kwargs):
    insert = f"INSERT INTO recursos (id, tipo) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.id},\'{row.tipo}\') "
        duplicate = f"ON CONFLICT (id) DO UPDATE SET tipo=excluded.tipo;\n"
        insertQuery += duplicate
    return insertQuery

# date insertion
def insert_query_fechas(**kwargs):
    insert = f"INSERT INTO fechas (id,anio) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.id},\'{row.anio}\') "
        duplicate = f"ON CONFLICT (id) DO UPDATE SET anio=excluded.anio;\n"
        insertQuery += duplicate
    return insertQuery

# employee insertion
def insert_query_actividades_mineras(**kwargs):
    insert = f"INSERT INTO actividades_mineras (id,geografia_id,recurso_id,fecha_id,unidad,cantidad) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.id},{row.geografia_id},{row.recurso_id},{row.fecha_id},\'{row.unidad}\',{row.cantidad}) "
        duplicate = f"ON CONFLICT (id) DO UPDATE SET geografia_id=excluded.geografia_id,recurso_id=excluded.recurso_id,fecha_id=excluded.fecha_id,unidad=excluded.unidad,cantidad=excluded.cantidad;\n"
        insertQuery += duplicate
    return insertQuery

# stock item insertion
def insert_query_homicidios(**kwargs):
    insert = f"INSERT INTO homicidios (id,geografia_id,fecha_id,cantidad) VALUES "
    insertQuery = ""
    dataframe =cargar_datos(kwargs['csv_path'])
    for index, row in dataframe.iterrows():
        insertQuery += insert + f"({row.id},{row.geografia_id},{row.fecha_id},{row.cantidad}) "
        duplicate = f"ON CONFLICT (id) DO UPDATE SET geografia_id=excluded.geografia_id,fecha_id=excluded.fecha_id,cantidad=excluded.cantidad;\n"
        insertQuery += duplicate
    return insertQuery
    

