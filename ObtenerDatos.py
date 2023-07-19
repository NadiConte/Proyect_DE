import requests
import os
from dotenv import load_dotenv
load_dotenv()

# una vez cargados los valores, podemos usarlos
API_KEY = os.getenv('API_KEY')

def obtenerDatos(numPeliculas=1000):
    peliculas = obtenerPeliculas(numPeliculas)

    for pelicula in peliculas:
        print("Título:", pelicula['titulo'])
        print("Año:", pelicula['anio'])
        print("Puntuación:", pelicula['puntuacion'])
        print("Clasificación:", pelicula['clasificacion'])
        print("Plataforma:", pelicula['plataforma'])
        print("Duración:", pelicula['duracion'], "minutos")
        print("Idioma original:", pelicula['idiomaOriginal'])
        print("Director:")
        print("Nombre:", pelicula['director']['nombre'])
        print("Fecha de nacimiento:", pelicula['director']['fechaNacimiento'])
        print("Nacionalidad:", pelicula['director']['nacionalidad'])
        print("Actores:")
        for actor in pelicula['actores']:
            print("Nombre:", actor['nombre'])
        print("-----------------------------------")

def obtenerPeliculas(numPeliculas):
    peliculas = []
    url = f'https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=es'
    response = requests.get(url)
    data = response.json()

    for pelicula in data['results'][:numPeliculas]:
        titulo = pelicula['title']
        anio = pelicula['release_date'][:4]
        puntuacion = pelicula['vote_average']
        clasificacion, plataforma, duracion, idiomaOriginal, director = obtenerInfoAdicional(pelicula['id'])
        actores = obtenerActores(pelicula['id'])
        peliculas.append({
            'titulo': titulo,
            'anio': anio,
            'puntuacion': puntuacion,
            'clasificacion': clasificacion,
            'plataforma': plataforma,
            'duracion': duracion,
            'idiomaOriginal': idiomaOriginal,
            'director': director,
            'actores': actores
        })
    return peliculas

def obtenerInfoAdicional(peliculaId):
    url = f'https://api.themoviedb.org/3/movie/{peliculaId}?api_key={API_KEY}&language=es&append_to_response=credits,watch/providers'
    response = requests.get(url)
    data = response.json()

    clasificacion = obtenerClasificacion(data)
    plataforma = obtenerPlataforma(data)
    duracion = obtenerDuracion(data)
    idiomaOriginal = obtenerIdiomaOriginal(data)
    director = obtenerDirector(data)

    return clasificacion, plataforma, duracion, idiomaOriginal, director

def obtenerClasificacion(data):
    clasificacion = "N/A"
    if 'release_dates' in data:
        for release in data['release_dates']['results']:
            if release['iso_3166_1'] == 'AR':
                clasificacion = release['certification']
                break
    return clasificacion

def obtenerPlataforma(data):
    plataforma = "N/A"
    if 'watch/providers' in data:
        proveedores = data['watch/providers']['results']
        for country in proveedores.keys():
            if 'flatrate' in proveedores[country]:
                plataforma = proveedores[country]['flatrate'][0]['provider_name']
                break
    return plataforma

def obtenerDuracion(data):
    duracion = "N/A"
    if 'runtime' in data:
        duracion = data['runtime']
    return duracion

def obtenerIdiomaOriginal(data):
    idiomaOriginal = "N/A"
    if 'original_language' in data:
        idiomaOriginal = data['original_language']
    return idiomaOriginal

def obtenerDirector(data):
    director = {}
    if 'credits' in data:
        for crewMember in data['credits']['crew']:
            if crewMember['job'] == 'Director':
                director['nombre'] = crewMember['name']
                director['fechaNacimiento'], director['nacionalidad'] = obtenerDatosPersona(crewMember['id'])
                break
    return director

def obtenerActores(peliculaId):
    url = f'https://api.themoviedb.org/3/movie/{peliculaId}/credits?api_key={API_KEY}&language=es'
    response = requests.get(url)
    data = response.json()
    actores = []
    for actor in data['cast']:
        actores.append({'nombre': actor['name']})
    return actores

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

if __name__ == '__main__':
    obtenerDatos()
