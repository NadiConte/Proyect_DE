# Proyect Data Engineering - CoderHouse

## Descripción
Este código permite realizar la ejecución de un DAG de Airflow llamado `obtener_datos_dag`.

### Lo que hace este DAG es:

1. Extrae datos de la API de [TMDB](https://developer.themoviedb.org/reference/intro/getting-started).
2. Guarda esos datos en una Base de Datos de Airflow (simulando un Data Warehouse).

### Configurar Credenciales 
Para poder correr el DAG, sera necesario crear un archivo .env  en la carpeta raíz del proyecto con la siguiente estructura:

```plaintext
API_KEY=mi_key_tmdb
DB_HOST=mi_host_redshift
DB_NAME=mi_db_redshift
DB_USER=mi_schema_redshift
DB_PASS=mi_pass_redshift
DB_PORT=mi_nro_puerto
```


## Pasos 

1. 
    ```bash
    docker-compose -f docker-compose.yaml build
    ```
2. 
    ```bash
    docker-compose -f docker-compose.yaml up
    ```
3. Entrar a [localhost:8080](http://localhost:8080/home) en el navegador que quiera.
4. Introduce como usuario y contraseña: `airflow`.
6. Una vez dentro, ejecute el DAG.
7. Cuando el DAG queda en color verde oscuro indica que fue ejecutado satisfactoriamente, puede corrobar esto ingresando a Redshift y validando que dentro del esquema correspondiente, en la tabla fact_table esten los nuevos registros.
7. Finalmente, cuando haya terminado, podrá liberar los recursos utilizados mediante el siguiente comando:
    ```bash
    docker-compose -f docker-compose.yaml down
    ```