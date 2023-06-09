# Airflow_docker_para_ETL

En el siguiente codigo se encontrará:
- Cómo conectarse a Airflow por medio de Docker.
- Generación de ETL (extracción, transformación y cargue) de datos de un problema de accidentes de la ciudad de Seattle mediante PythonOperators

## *Cómo usar el proyecto desde disco local*

Para ello es requerido realizar los siguientes pasos:
- Descargar Docker Desktop
- Generar una carpeta (en mi caso la llame airflow-docker) y dentro del disco local, posterior a ello, generar las siguientes carpetas dentro:
  - dags
  - logs
  - plugins
- Posteriormente, se debe descargar el archivo: **docker-compose.yaml** y ponerlo en la carpeta principal.
- Por último, se debe descargar el archivo **my_dag.py** e ingresarlo en la carpeta **dags** dentro de este archivo se puede encontrar:
  - Scripts para generar cada tarea solicitada
  - DAG para automatización del proceso y secuencia de ejecución.

Para poder ejecutar el programa se debe correr las siguientes líneas en el terminal de la carpeta asociada:

```docker-compose up airflow-init```

Una vez ejecutado se debe generar la siguinte línea en el terminal:

```docker-compose up```


## *Cómo modificar de dónde extraer el archivo y donde cargarlo*

Se debe modificar dentro del codigo de python la ruta donde se guarda la data que se analizó y la data generada para que sea cargada a la ruta especificada por el usuario:

Donde menciona ```/File_Location/``` por favor indicar la locación donde desea leer su archivo base y guardar la solución final.

## *Cómo correr el archivo en Airflow*

Para ingresar a la aplicación de Airflow donde se encuentra la solución al proyecto se debe colocar la siguiente ruta en el navegador:

```localhost:8080```

Posterior a ello nos solicitará un usuario y contraseña:

```
Usario: airflow
Contraseña: airflow
```

Una vez allí se debe buscar el proyecto titulado ***ETL*** y al ingresar se puede observar la solución de dos maneras:
- Automatico diariamente por el sistema.
- Manual al presionar en un botón de "play" y seleccionando la opción ```Trigger DAG```

De esta forma se puede ejecutar todo el proyecto en cuestión.


