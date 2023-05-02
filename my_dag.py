
"""
Librerias necesarias para ejecutar la tarea solicitada
"""

import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

#Funciones con la que se ejecutara el codigo

def Leer_datos(archivo):
    """
    Leer datos del problema en cuestión
    """

    data = pd.read_csv(archivo)
    return data

def Eliminar_Faltantes(**context):
    """
    Eliminar NULL
    """
    data=context['task_instance'].\
        xcom_pull(task_ids='Leer_csv') #Toma información de la tarea anterior
    data=data.dropna().reset_index(drop=True) #Elimina los faltantes y resetea el index
    return data

def Cantidad_Accidentes(**context):
    """
    Agrupar para conocer cantidad de accidentes segun solicitado
    """
    
    data=context['task_instance'].\
        xcom_pull(task_ids='Eliminar_faltantes') #Toma información de la tarea anterior
    col = 'YEAR' #Columna, como nos se encontro la de clima se uso la de año
    df = data.loc[:,[col, 'OBJECTID']] #Sólo se mira columna objetivo y columnas con datos únicos para contar
    df_FINAL = pd.DataFrame(df.groupby(col).count()) #Se agrupa por lo solicitado
    df_FINAL = df_FINAL.rename_axis(col).reset_index() #se formatea el index
    df_FINAL = df_FINAL.rename(columns={'OBJECTID':'CANT_ACCIDENTES'}) #Se cambia nombre de columna
    return df_FINAL

def Exportar_Solucion(archivo_salida, **context):
    """
    Exportar solucion
    """
    df_FINAL=context['task_instance'].\
        xcom_pull(task_ids='Transformacion_data') #Toma información de la tarea anterior
    df_FINAL.to_csv(archivo_salida, index=False)



#Creación del DAG y ejecución del mismo

"""
Se crea el dag y que se ejecute diariamente a las 00:00
"""
dag = DAG(
    'ETL',
    start_date=datetime(2023, 5, 1, 0, 0, 00000),
    schedule_interval='@daily', #ejecucion diaria
    catchup=False
)


"""
Tarea 1: Leer el csv
"""
t1 = PythonOperator(
    task_id='Leer_csv',
    provide_context=True,
    python_callable=Leer_datos,
    op_kwargs={"archivo": "/File_Location/Traffic_Flow_Map_Volumes.csv"},
    dag=dag
)
#'./airflow-local/Traffic_Flow_Map_Volumes.csv'

"""
Tarea 2: Eliminar datos faltantes
"""
t2 = PythonOperator(
    task_id='Eliminar_faltantes',
    provide_context=True,
    python_callable=Eliminar_Faltantes,
    dag=dag
)

"""
Tarea 3: Transformar datos a como fue solicitado
"""
t3 = PythonOperator(
    task_id='Transformacion_data',
    provide_context=True,
    python_callable=Cantidad_Accidentes,
    dag=dag
)

"""
Tarea 4: Generar csv
"""
t4 = PythonOperator(
    task_id='Generar_csv',
    provide_context=True,
    python_callable=Exportar_Solucion,
    op_kwargs={"archivo_salida": "/File_Location/Solucion.csv"},
    dag=dag
)

#Generar secuencia de ejecucion
t1 >> t2 >> t3 >> t4