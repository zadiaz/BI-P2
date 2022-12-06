import numpy as np
import pandas as pd
import os


def cargar_datos(name):
    df = pd.read_csv("data/" + name + ".csv", sep=',', encoding='latin1', index_col=False)
    return df


def guardar_datos(df, nombre):
    df.to_csv("data/" + nombre + ".csv", encoding='latin1', sep=',', index=False)


import pandas as pd
import os


def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding='latin1', index_col=False)
    return df


def guardar_datos(df, nombre):
    df.to_csv(nombre + ".csv", encoding='latin1', sep=',', index=False)

def procesar_datos1(df):
    columns = ['Departamento', 'Codigo Entidad', 'Entidad', 'Indicador', 'Dato Numerico', 'Ano', 'Unidad de Medida']
    df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df = df[df.Mes == 12]
    df = df[columns]
    df['Dato Numerico'] = df['Dato Numerico'].str.replace(r'.', '').str.replace(r',', '.')
    df['Dato Numerico'] = pd.to_numeric(df['Dato Numerico'], errors='coerce').fillna(0).astype(
        np.float64)

    return df

def procesar_datos_mineria(df):
    df = pd.read_json('mineriaInfo.json')
    df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    df.trimestre = pd.to_numeric(df.trimestre.str.replace(r'Trimestre ', ''), errors='coerce').fillna(
        0).astype(
        np.int64)
    df = df[df.trimestre == 4]

    return df


def procesar_datos():
    ## Dimension city
    salud = pd.read_excel("Salud.xlsx", nrows = 5000
       )
    salud = procesar_datos1(salud)

    dem = pd.read_excel("Demografía Y Poblacion.xlsx", nrows = 5000
       )
    dem = procesar_datos1(dem)

    mineria = pd.read_json("mineriaInfo.json")

    mineria = procesar_datos_mineria(mineria)

    join_dem_salud = pd.merge(dem, salud,
                              on=["Departamento", "Codigo Entidad", "Ano", "Indicador", "Unidad de Medida"],
                              how='inner')
    join_dem_salud = join_dem_salud.drop_duplicates(
        ["Departamento", "Codigo Entidad", "Ano", "Indicador", "Unidad de Medida"], keep='last')

    inner_join = pd.merge(mineria, join_dem_salud, how='inner',
                          left_on=['departamento', 'codigo_dane', 'a_o_produccion'],
                          right_on=['Departamento', 'Codigo Entidad', 'Ano'])

    inner_join = inner_join[['codigo_dane', 'municipio_productor', 'departamento', 'recurso_natural',
                             'nombre_del_proyecto', 'a_o_produccion', 'unidad_medida',
                             'cantidad_producci_n',
                             'Indicador', 'Dato Numerico_x', 'Unidad de Medida', 'Entidad_y',
                             'Dato Numerico_y']]

    dep_cols = ['codigo_dane', 'municipio_productor', 'departamento']
    department = inner_join.groupby(dep_cols).count().reset_index()[dep_cols]


    anio = inner_join.groupby(['a_o_produccion']).count().reset_index()[['a_o_produccion']]

    activiad_cols = ['recurso_natural',
          'nombre_del_proyecto', 'unidad_medida',
          'cantidad_producci_n']
    actividad_minera = inner_join.groupby(

        activiad_cols
    ).count().reset_index()[activiad_cols]

    salud_cols = ['Indicador',
          'Unidad de Medida', 'Entidad_y',
          'Dato Numerico_y']
    salud = inner_join.groupby(salud_cols).count().reset_index()[salud_cols]


    dem_cols = ['Indicador', 'Dato Numerico_x',
          'Unidad de Medida', 'Entidad_y'
          ]
    dem = inner_join.groupby(dem_cols).count().reset_index()[dem_cols]

    inner_join['depFK'] = 0
    inner_join['saludFK'] = 0
    inner_join['anioFK'] = 0
    inner_join['mineriaFK'] = 0

    for idx in inner_join.index:
        depFK = department.index[
            (department['codigo_dane'] == inner_join['codigo_dane'][idx]) &
            (department['municipio_productor'] == inner_join['municipio_productor'][idx]) &
            (department['departamento'] == inner_join['departamento'][idx])
            ][0]

        anioFK = anio.index[
            (anio['a_o_produccion'] == inner_join['a_o_produccion'][idx])
            ][0]

        mineriaFK = actividad_minera.index[
            (actividad_minera['recurso_natural'] == inner_join['recurso_natural'][idx]) &
            (actividad_minera['nombre_del_proyecto'] == inner_join['nombre_del_proyecto'][idx]) &
            (actividad_minera['unidad_medida'] == inner_join['unidad_medida'][idx]) &
            (actividad_minera['cantidad_producci_n'] == inner_join['cantidad_producci_n'][idx])

            ][0]

        saludFK = salud.index[
            (salud['Indicador'] == inner_join['Indicador'][idx]) &
            (salud['Unidad de Medida'] == inner_join['Unidad de Medida'][idx]) &
            (salud['Entidad_y'] == inner_join['Entidad_y'][idx]) &
            (salud['Dato Numerico_y'] == inner_join['Dato Numerico_y'][idx])

            ][0]

        demFK = dem.index[
            (dem['Indicador'] == inner_join['Indicador'][idx]) &
            (dem['Unidad de Medida'] == inner_join['Unidad de Medida'][idx]) &
            (dem['Entidad_y'] == inner_join['Entidad_y'][idx]) &
            (dem['Dato Numerico_x'] == inner_join['Dato Numerico_x'][idx])

            ][0]



        inner_join.at[idx, 'depFK'] = depFK
        inner_join.at[idx, 'saludFK'] = saludFK
        inner_join.at[idx, 'anioFK'] = anioFK
        inner_join.at[idx, 'mineriaFK'] = mineriaFK
        inner_join.at[idx, 'demFK'] = demFK

        inner_join["access"] = inner_join['Dato Numerico_x'] / inner_join['Dato Numerico_y']

        facts_table = inner_join[['depFK', 'saludFK', 'anioFK', 'mineriaFK', 'demFK', 'access']]

        guardar_datos(department, 'department')
        guardar_datos(salud, 'salud')
        guardar_datos(anio, 'anio')
        guardar_datos(mineria, 'mineria')
        guardar_datos(dem, 'dem')
        guardar_datos(facts_table, 'facts_table')




def insert_query_city(**kwargs):
    insert = f"INSERT INTO city (City_Key,City,State_Province,Country,Continent,Sales_Territory,Region,Subregion,Latest_Recorded_Population) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.City_Key},\'{row.City}\',\'{row.State_Province}\',\'{row.Country}\',\'{row.Continent}\',\'{row.Sales_Territory}\',\'{row.Region}\',\'{row.Subregion}\',{row.Latest_Recorded_Population});\n"
        return insertQuery
    except:
        return ""

procesar_datos()