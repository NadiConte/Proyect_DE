from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pandas import read_sql_query
import sys
import os
import pandas as pd
import requests
# from ..functions.ObtenerDatos import main

# from functions.ObtenerDatos import main

# Agregar la ruta de proyecto_de al sys.path
proyecto_de_path = os.path.abspath(os.path.join(os.path.dirname(sys.path[0]), "Proyect_DE"))
sys.path.insert(0, proyecto_de_path)


# Cargar variables de entorno desde el archivo .env
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

# Variables de conexión a Redshift y API_KEY
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_PORT = os.getenv('DB_PORT')
API_KEY = os.getenv('API_KEY')


def crear_tablas():
    # Crear las tablas en Redshift
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    conn = create_engine(conn_str)

    sql_script_path = './scripts/creartablas.sql'
    with conn.connect() as connection:
        with open(sql_script_path, 'r') as f:
            sql_script = f.read()
        connection.execute(sql_script)


load_dotenv()

API_KEY = os.getenv('API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_PORT = os.getenv('DB_PORT')

fechas_data = []

def obtenerDatos(numPeliculas=15):
    peliculas = obtenerPeliculas(numPeliculas)

    peliculas_data = []
    directores_data = []
    hechos_data = []

    fecha_actual = datetime.today().date()
    anio = fecha_actual.year
    mes = fecha_actual.month
    dia = fecha_actual.day

    fechas_data.append({
        'id_tiempo': fecha_actual,
        'anio': anio,
        'mes': mes,
        'dia': dia
    })

    for pelicula in peliculas:
        peliculas_data.append({
            'id_pelicula': pelicula['id'],
            'titulo': pelicula['titulo'],
            'anio': pelicula['anio'],
            'puntuacion': pelicula['puntuacion'],
            'duracion': pelicula['duracion'],
            'idioma_original': pelicula['idiomaOriginal'],
            'presupuesto' : pelicula['presupuesto']
        })

        directores_data.append({
            'id_director': pelicula['director']['id_director'],
            'nombre': pelicula['director']['nombre'],
            'fecha_nacimiento': pelicula['director']['fechaNacimiento'],
            'nacionalidad': pelicula['director']['nacionalidad']
        })

        hechos_data.append({
            'id_pelicula': pelicula['id'],
            'id_director': pelicula['director']['id_director'],
            'id_tiempo': fecha_actual,
            'prom_puntuacion': pelicula['prom_puntuacion'],
            'cant_votos' : pelicula['cant_votos'],
            'popularidad' : pelicula['popularidad'],
            'recaudacion' : pelicula['recaudacion']
        })

    peliculas_df = pd.DataFrame(peliculas_data).drop_duplicates()
    directores_df = pd.DataFrame(directores_data).drop_duplicates()
    fecha_df = pd.DataFrame(fechas_data).drop_duplicates()
    hechos_df = pd.DataFrame(hechos_data).drop_duplicates()

    return peliculas_df, directores_df, fecha_df, hechos_df

def obtenerPeliculas(numPeliculas):
    #themoviedb solo me permite consultar 20 peliculas por pagina
    peliculas = []
    num_paginas = (numPeliculas - 1) // 20 + 1  #Calculo el nro de páginas necesarias para la cant de peliculas ingresadas
    for pagina in range(1, num_paginas + 1):
        url = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=es&page={pagina}'
        response = requests.get(url)
        data = response.json()

        for pelicula in data['results']:
            popularidad, presupuesto, recaudacion, cant_votos, prom_puntuacion, duracion, idiomaOriginal, director = obtenerInfoAdicional(pelicula['id'])
            peliculas.append({
                'id':pelicula['id'],
                'titulo': pelicula['title'],
                'anio': pelicula['release_date'][:4],
                'puntuacion': pelicula['vote_average'],
                'popularidad' : popularidad,
                'presupuesto' : presupuesto,
                'recaudacion' : recaudacion,
                'cant_votos' : cant_votos,
                'prom_puntuacion' : prom_puntuacion,
                'duracion': duracion,
                'idiomaOriginal': idiomaOriginal,
                'director': director
            })

    return peliculas[:numPeliculas]

def obtenerInfoAdicional(peliculaId):
    url = f'https://api.themoviedb.org/3/movie/{peliculaId}?api_key={API_KEY}&language=es&append_to_response=credits,watch/providers'
    response = requests.get(url)
    data = response.json()

    popularidad = data['popularity'] if 'popularity' in data else 'Desconocido'
    presupuesto = data['budget'] if 'budget' in data else 'Desconocido'
    recaudacion = data['revenue'] if 'revenue' in data else 'Desconocido'
    cant_votos = data['vote_count'] if 'vote_count' in data else 'Desconocido'
    prom_puntuacion = data['vote_average'] if 'vote_average' in data else 'Desconocido'
    duracion = data['runtime'] if 'runtime' in data else 'Desconocida'
    idiomaOriginal = obtenerIdiomaOriginal(data)
    director = obtenerDirector(data)

    return popularidad, presupuesto, recaudacion, cant_votos, prom_puntuacion, duracion, idiomaOriginal, director

def obtenerDirector(data):
    director = {}
    if 'credits' in data:
        for crewMember in data['credits']['crew']:
            if crewMember['job'] == 'Director':
                director['id_director'] = crewMember['id']
                director['nombre'] = crewMember['name']
                director['fechaNacimiento'], director['nacionalidad'] = obtenerDatosPersona(crewMember['id'])
                break
    return director

def obtenerDatosPersona(personaId):
    url = f'https://api.themoviedb.org/3/person/{personaId}?api_key={API_KEY}&language=es'
    response = requests.get(url)
    data = response.json()
    fechaNacimiento = data['birthday']
    nacionalidad = obtenerPais(data.get('place_of_birth'))
    return fechaNacimiento, nacionalidad

def obtenerPais(lugarNacimiento):
    if lugarNacimiento:
        pais = lugarNacimiento.split(', ')
        return pais[-1]
    return 'N/A'

def obtenerIdiomaOriginal(data):
    idiomaOriginal = 'Desconocido'
    if 'spoken_languages' in data:
        idiomas_disponibles = data['spoken_languages']
        if idiomas_disponibles:
            idiomaOriginal = idiomas_disponibles[0].get('name', 'Desconocido')
    return idiomaOriginal

def cargarDatosEnRedshift(peliculas_df, directores_df, fecha_df, hechos_df):
#    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Engine para la conexion a Redshift
    conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    conn = create_engine(conn_str)


    # Cargar los datos actuales de las tablas de Redshift en DataFrames
    df_actual_peliculas = read_sql_query('SELECT id_pelicula FROM nadina_conte_coderhouse.dim_peliculas',con = conn)
    df_actual_directores = read_sql_query('SELECT id_director FROM nadina_conte_coderhouse.dim_directores',con = conn)
    df_actual_hechos = read_sql_query('SELECT id_pelicula,id_director,id_tiempo FROM nadina_conte_coderhouse.fact_table',con = conn)

    # Identificar nuevos registros a insertar en cada tabla
    df_peliculas_nuevas = peliculas_df.merge(df_actual_peliculas, on='id_pelicula', how='left', indicator=True)
    df_peliculas_nuevas = df_peliculas_nuevas[df_peliculas_nuevas['_merge'] == 'left_only'].drop('_merge', axis=1)


    df_directores_nuevos = directores_df.merge(df_actual_directores, on='id_director', how='left', indicator=True)
    df_directores_nuevos = df_directores_nuevos[df_directores_nuevos['_merge'] == 'left_only'].drop('_merge', axis=1)

    df_hechos_nuevos = hechos_df.merge(df_actual_hechos, on=['id_director', 'id_pelicula', 'id_tiempo'], how='left',
                                       indicator=True)
    df_hechos_nuevos = df_hechos_nuevos[df_hechos_nuevos['_merge'] == 'left_only'].drop('_merge', axis=1)


    # Mostrar datos
    # DataFrame para fact_table
    print("DataFrame hechos:")
    print(df_hechos_nuevos)
    # DataFrame para tabla película
    print("\nDataFrame peliculas:")
    print(df_peliculas_nuevas)
    # DataFrame para tabla directores
    print("\nDataFrame directores:")
    print(df_directores_nuevos)
    # DataFrame para tabla fecha
    print("\nDataFrame fecha:")
    print(fecha_df)

    # Insertar los nuevos registros en las tablas de Redshift
    df_peliculas_nuevas.to_sql(name='dim_peliculas', con=conn, schema='nadina_conte_coderhouse', if_exists='append',
                               index=False)
    df_directores_nuevos.to_sql(name='dim_directores', con=conn, schema='nadina_conte_coderhouse', if_exists='append',
                                index=False)
    fecha_df.to_sql(name='dim_tiempo', con=conn, schema='nadina_conte_coderhouse', if_exists='append', index=False)
    df_hechos_nuevos.to_sql(name='fact_table',con=conn,schema='nadina_conte_coderhouse',if_exists='append',index=False)

def main():
    # Opciones para mostrar completos los distintos DFs
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    #Obtener datos para las distintas tablas
    peliculas_df, directores_df, fecha_df, hechos_df = obtenerDatos()
    # Cargar datos en Redshift
    cargarDatosEnRedshift(peliculas_df, directores_df, fecha_df, hechos_df)



# Definición del DAG
default_args = {
    'owner': 'Nadina Conte',
    'start_date': datetime(2023,8,21),
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

with DAG('obtener_datos_dag',
          description='DAG para obtener y cargar datos',
          schedule_interval='0 12 * * *',
          catchup=False,
          default_args=default_args) as dag:

    def ejecutar_main(**kwargs):
        main()

    ObtenerDatos = PythonOperator(
        task_id='ejecutar_main',
        python_callable=ejecutar_main,
        provide_context=True,
        dag=dag
    )   

    ObtenerDatos