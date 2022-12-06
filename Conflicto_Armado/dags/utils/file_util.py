import pandas as pd
#import js2py

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding = 'latin1', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/' + nombre + '.csv' , encoding = 'latin1', sep=',', index=False)
    


def procesar_datos():

   # js2py.run_file("index.js")


    municipios = pd.read_json('dimension_municipios.json')
    guardar_datos(municipios, "dimension_municipios")

    recursos = pd.read_json('dimension_recursos.json')
    guardar_datos(recursos, "dimension_recursos")

    fechas = pd.read_json('dimension_fechas.json')
    guardar_datos(fechas, "dimension_fechas")

    homicidios = pd.read_json('dimension_homicidios.json')
    guardar_datos(homicidios, "dimension_homicidios")

    actividades_mineras = pd.read_json('dimension_actividades_mineras.json')
    guardar_datos(actividades_mineras, "dimension_actividades_mineras")



