import os
import requests
import pandas as pd
from dotenv import load_dotenv
import datetime
from sqlalchemy  import create_engine

load_dotenv()

API_KEY = os.getenv('API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_PORT = os.getenv('DB_PORT')

def obtenerDatos(numPeliculas=50):
    peliculas = obtenerPeliculas(numPeliculas)

    peliculas_data = []
    directores_data = []
    fechas_data = []
    hechos_data = []

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

        fecha_actual = datetime.date.today()
        fechas_data.append({
            'id_tiempo': fecha_actual,
            'anio': fecha_actual.year,
            'mes': fecha_actual.month,
            'dia': fecha_actual.day
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
    df_actual_peliculas = pd.read_sql_table('dim_peliculas', conn, schema='nadina_conte_coderhouse')
    df_actual_directores = pd.read_sql_table('dim_directores', conn, schema='nadina_conte_coderhouse')

    # Identificar nuevos registros a insertar en cada tabla
    peliculas_nuevas = peliculas_df[~peliculas_df['id_pelicula'].isin(df_actual_peliculas['id_pelicula'])]
    directores_nuevos = directores_df[~directores_df['id_director'].isin(df_actual_directores['id_director'])]

    # Insertar los nuevos registros en las tablas de Redshift
    peliculas_nuevas.to_sql(name='dim_peliculas', con=conn, schema='nadina_conte_coderhouse',
                            if_exists='append', index=False)
    directores_nuevos.to_sql(name='dim_directores', con=conn, schema='nadina_conte_coderhouse',
                             if_exists='append', index=False)
    fecha_df.to_sql(name='dim_tiempo', con=conn, schema='nadina_conte_coderhouse', if_exists='append', index=False)
    hechos_df.to_sql(name='fact_table', con=conn, schema='nadina_conte_coderhouse', if_exists='append', index=False)

if __name__ == '__main__':
    # Opciones para mostrar completos los distintos DFs
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    #Obtener datos para las distintas tablas
    peliculas_df, directores_df, fecha_df, hechos_df = obtenerDatos()

    #Mostrar datos
    # DataFrame para fact_table
    print("DataFrame hechos:")
    print(hechos_df)
    # DataFrame para tabla película
    print("\nDataFrame peliculas:")
    print(peliculas_df)
    # DataFrame para tabla directores
    print("\nDataFrame directores:")
    print(directores_df)
    # DataFrame para tabla fecha
    print("\nDataFrame fecha:")
    print(fecha_df)

    # Cargar datos en Redshift
    cargarDatosEnRedshift(peliculas_df, directores_df, fecha_df, hechos_df)