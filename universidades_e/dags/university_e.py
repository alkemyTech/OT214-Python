import os
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
import unidecode
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dateutil.relativedelta import relativedelta
from decouple import config

from config.logging_config import lg_connect, lg_process, lg_send, path

# dag default arguments and retries
default_arguments = {
    'owner': 'Maxi Cabrera',
    'start_date': datetime(2022, 5, 1, 00, 00),
    'description': "dag etl proccesed information to universitys",
    'retry_delay': timedelta(minutes=2)

}


# dag start run every hour
with DAG(
    dag_id='university_e',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_arguments,
)as dag:

    # function of connection
    def conn_query():
        database = config("_PG_DATABASE")
        user = config('_PG_USERNAME')
        password = config('_PG_PASSWORD')
        host = config('_PG_HOST')
        port = config('_PG_PORT')

        lg_connect.info('initializing DAG connect.')

        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://'
                                          f'{user}:{password}@{host}:'
                                          f'{port}/{database}')

        conn = engine.connect()
        lg_connect.info('Database connect successfuly.')

        # Db query
        # creating the path to sql files
        path_inter = f'{path}/sql/abierta_interamericana.sql'
        path_pampa = f'{path}/sql/nacional_de_la_pampa.sql'
        path_files = f'{path}/files'
        if not os.path.exists(path_files):
            os.makedirs(path_files)
            lg_connect.info('the files folder was created')

        # reading the content of sql file
        read_inter = open(path_inter, 'r', encoding='utf-8').read()
        read_pampa = open(path_pampa, 'r', encoding='utf-8').read()
        lg_connect.info('read files.')

        # generating query with pandas
        df_query_inter = pd.read_sql(read_inter, conn)
        df_query_pampa = pd.read_sql(read_pampa, conn)
        lg_connect.info('reading sql with pandas')

        # generating the csv files
        df_query_inter.to_csv(f'{path}/files'
                              '/universidad_abierta_interamericana.csv',
                              index=False)
        df_query_pampa.to_csv(f'{path}/files'
                              '/universidad_nacional_de_la_pampa.csv',
                              index=False)
        lg_connect.info('csv files created.')

    # process data function
    def proccess():
        lg_process.info('initializing DAG connect.')
        # funcion de normalizacion universidad nacional de la pampa
        # leer csv y guardar en dataframes

        def u_nacional_la_pampa():
            f_csv_pampa = (f'{path}/files/universidad_nacional'
                           '_de_la_pampa.csv')

            lg_process.info('initializing process whith '
                            'national university of the pampa.')

            df_origin_pampa = pd.read_csv(f_csv_pampa)

            df_names = df_origin_pampa['nombrre'].str.split()

            # saco los prefijo de la primer palabras
            prefix = ['miss', 'dr.', 'ms.', 'mr.', 'mrs.']
            for name in df_names:
                if len(name) >= 3:
                    borrar = []
                    if len(name[0]) <= 4:
                        for pre in prefix:
                            if pre in (name[0].lower()):
                                name[1] = f'{name[0]}_{name[1]}'
                                borrar.append(0)

                    # si la tecer palabra solo tiene 3 letras las uno
                    # con la segunda
                    if len(name[2]) <= 3:
                        if name[2] == 'lee':
                            print('')
                        else:
                            name[1] = f'{name[1]}_{name[2]}'
                            borrar.append(2)
                    # si hay una tercer palabra y tiene menos de 3 letras
                    # la uno a la segunda
                    if len(name) > 3:
                        if len(name[3]) <= 3:
                            name[2] = f'{name[2]}_{name[3]}'
                            borrar.append(3)

                    # para no modificar lo indices en la busqueda
                    # borro al final
                    borrar.sort(reverse=True)
                    for elemento in borrar:
                        name.pop(elemento)

                    borrar = []
            # creo una lista con los valores del dtararame modificado
            nuevo_df = []
            for palabra in df_names:
                nuevo_df.append(palabra)

            # genero un nuevo dataframe con nombre y apellido
            df_names = pd.DataFrame(nuevo_df)
            df_names.columns = ['first_name', 'last_name']

            # concateno los dataframes
            df_a_names_last = pd.concat([df_origin_pampa, df_names], axis=1)

            # borro la columna nombres original
            del(df_a_names_last['nombrre'])

            # acomodo columnas para renombrear
            df_a_names_last = df_a_names_last[[
                'universidad', 'carrerra', 'fechaiscripccion',
                'first_name', 'last_name', 'sexo', 'nacimiento',
                'codgoposstal', 'direccion', 'eemail'
            ]]

            # renombro columnas
            df_a_names_last.columns = [
                'university', 'carrer', 'inscription_date',
                'first_name', 'last_name', 'gender', 'age',
                'postal_code', 'location', 'email'
            ]

            # pasar columnas a minuscula
            for columna in df_a_names_last:
                if columna == 'postal_code':
                    continue
                elif columna == 'email':
                    continue
                else:
                    df_a_names_last[columna] = \
                        df_a_names_last[columna].str.lower()

            # formateo columna de fecha
            df_a_names_last['inscription_date'] = \
                pd.to_datetime(df_a_names_last['inscription_date'])
            # cambio el tipo de dato a obj
            df_a_names_last['inscription_date'] = \
                df_a_names_last['inscription_date'].astype(object)

            df_a_names_last['postal_code'] = \
                df_a_names_last['postal_code'].astype(object)

            # formateo columna gender
            df_a_names_last['gender'] = \
                df_a_names_last['gender'].str.replace('m', 'male')

            df_a_names_last['gender'] = \
                df_a_names_last['gender'].str.replace('f', 'female')

            # formateo columna age
            df_a_names_last['age'] = \
                pd.to_datetime(df_a_names_last['age'], format='%d/%m/%Y')

            # tranformo fecha en edad
            edad = 0
            f_actual = datetime.now()

            for i in range(0, (len(df_a_names_last.index))):
                f_nacimiento = df_a_names_last.loc[i, 'age']

                edad = (f_actual.year) - (f_nacimiento.year)

                if (f_actual.month) == (f_nacimiento.month):
                    if (f_actual.day) >= (f_nacimiento.day):
                        edad = edad + 1

                if (f_actual.month) > (f_nacimiento.month):
                    edad = edad + 1
                # cambio el valor de las filas por la edad
                df_a_names_last.loc[i, 'age'] = edad

            # cambio el tipo de dato de la columna a int
            df_a_names_last['age'] = \
                df_a_names_last['age'].astype(int)

            # saco guiones de la columna first names y last names
            df_a_names_last['first_name'] = \
                df_a_names_last['first_name'].str.replace('_', ' ', regex=True)

            df_a_names_last['last_name'] = \
                df_a_names_last['last_name'].str.replace('_', ' ', regex=True)

            # saco comas y puntos de la columna location
            df_a_names_last['location'] = \
                df_a_names_last['location'].str.replace(',', '', regex=True)

            df_a_names_last['location'] = \
                df_a_names_last['location'].str.replace('.', '', regex=True)

            # saco acentos y Ñ de la columna carrer
            for i in range(0, len(df_a_names_last.index)):
                valor = df_a_names_last.loc[i, 'carrer']
                valor = unidecode.unidecode(valor)
                df_a_names_last.loc[i, 'carrer'] = valor

            # pasar a archivo txt
            df_a_names_last.to_csv(f'{path}/files/universidad_'
                                   'nacional_de_la_pampa.txt',
                                   index=None, sep=',', mode='a')

            lg_process.info('txt file generated')

            lg_process.info('finish process whith '
                            'national university of the pampa succesfuly.')

        def open_interamerican_u():
            f_csv_inter = (f'{path}/files/universidad_abierta'
                           '_interamericana.csv')

            f_postal_code = (f'{path}/files/codigos_postales.csv')

            lg_process.info('initializing process whith '
                            'open interamerican university.')

            df_origin_inter = pd.read_csv(f_csv_inter)

            df_postal_code = pd.read_csv(f_postal_code)

            # sacar guiones de la columna y espacios en blanco al inicio y fin
            def q_guion(df, columna):
                df[columna] = \
                    df[columna].str.replace('-', ' ', regex=True)

                df[columna] = \
                    df[columna].str.strip()

            def q_dot(df, columna):
                df[columna] = \
                    df[columna].str.replace(',.', '', regex=True)

                df[columna] = \
                    df[columna].str.replace('.', '', regex=True)

            # saca mayusculas
            def q_mayuscula(df, columna):
                df[columna] = \
                    df[columna].str.lower()

            # saco acentos y ñ
            def q_acentos(df, columna):
                for i in range(0, len(df.index)):
                    valor = df.loc[i, columna]
                    valor = unidecode.unidecode(valor)
                    df.loc[i, columna] = valor

            def t_gender(df, columna):
                df[columna] = \
                    df[columna].str.replace('m', 'male', case=False)

                df[columna] = \
                    df[columna].str.replace('f', 'female', case=False)

            def t_name(columna):
                # saco los . y -
                df_origin_inter[columna] = \
                    df_origin_inter[columna].str.replace('.-', ' ', regex=True)

                # separar y agrupar nombres separados
                for i in range(0, len(df_origin_inter.index)):
                    valor = df_origin_inter.loc[i, columna]
                    valor = valor.split()
                    if len(valor) > 1:
                        if len(valor[0]) <= 2:
                            if (valor[0]) == 'am':
                                print('')
                            else:
                                valor[1] = f'{valor[0]}.{valor[1]}'
                                valor.pop(0)
                                valor1 = " ".join(valor)
                                df_origin_inter.loc[i, columna] = valor1

                        if len(valor) > 2:
                            if int(len(valor[2])) <= 2:
                                valor[1] = f'{valor[1]}-{valor[2]}'
                                valor.pop(2)
                                valor2 = " ".join(valor)
                                df_origin_inter.loc[i, columna] = valor2
                df_origin_inter[['first_name', 'last_name']] = \
                    df_origin_inter[columna].str.split(' ', expand=True)

                # borro columna nombres
                del(df_origin_inter[columna])

                # saco puntos de la columna first name
                df_origin_inter['first_name'] = \
                    df_origin_inter['first_name'].str.replace('.', ' ',
                                                              regex=True)

            def t_to_age(columna):
                df_origin_inter[columna] = \
                    pd.to_datetime(df_origin_inter[columna], format='%y/%b/%d')

                # tranformo fecha en edad
                edad = 0
                f_year = datetime(2003, 1, 1)
                f_actual = datetime.now()

                for i in range(0, (len(df_origin_inter.index))):
                    f_nacimiento = df_origin_inter.loc[i, columna]
                    # resto 100 años a los años invalidos.
                    if df_origin_inter.loc[i, columna] >= f_year:
                        f_nacimiento = df_origin_inter.loc[i, columna]
                        - relativedelta(years=100)

                    edad = (f_actual.year) - (f_nacimiento.year)
                    if (f_actual.month) == (f_nacimiento.month):
                        if (f_actual.day) >= (f_nacimiento.day):
                            edad = edad + 1

                    if (f_actual.month) > (f_nacimiento.month):
                        edad = edad + 1

                    # cambio el valor de las filas por la edad
                    df_origin_inter.loc[i, columna] = edad

                # cambio el tipo de dato de la columna a int
                df_origin_inter[columna] = \
                    df_origin_inter[columna].astype(int)

            def g_postal_code(df, columna):
                code = []
                for index in range(0, len(df.index)):
                    for c_index in range(0, len(df_postal_code.index)):
                        if (df.loc[index, columna]) ==\
                                                      (df_postal_code.loc
                                                       [c_index, 'localidad']):
                            code.append(str(df_postal_code.loc
                                            [c_index, 'codigo_postal']))
                            continue

                    df.loc[index, columna] = ' '.join(code)
                    code = []

            q_guion(df_origin_inter, 'univiersities')

            q_guion(df_origin_inter, 'carrera')

            q_mayuscula(df_origin_inter, 'carrera')

            q_acentos(df_origin_inter, 'carrera')

            q_mayuscula(df_origin_inter, 'names')

            # separo la columna name en first_name y last_name
            t_name('names')

            # saco guiones de la columna last_name
            q_guion(df_origin_inter, 'last_name')

            # tranformo m en male y f en female
            t_gender(df_origin_inter, 'sexo')

            # transformar fecha de nacimiento en edad
            t_to_age('fechas_nacimiento')

            # saco giones
            q_guion(df_origin_inter, 'direcciones')

            # saco , y .
            q_dot(df_origin_inter, 'direcciones')

            q_guion(df_origin_inter, 'localidad')

            q_mayuscula(df_origin_inter, 'localidad')

            q_mayuscula(df_postal_code, 'localidad')

            g_postal_code(df_origin_inter, 'localidad')

            df_origin_inter = df_origin_inter[[
                'univiersities', 'carrera', 'inscription_dates',
                'first_name', 'last_name', 'sexo',
                'fechas_nacimiento', 'localidad',
                'direcciones', 'email'
            ]]

            df_origin_inter.columns = [
                'university', 'carrer', 'inscription_date',
                'first_name', 'last_name', 'gender', 'age',
                'postal_code', 'location', 'email'
            ]

            # paso el dataframe al archivo txt
            df_origin_inter.to_csv(f'{path}/files/'
                                   f'universidad_abierta_interamericana.txt',
                                   index=None, sep=',', mode='a')

            lg_process.info('txt file generated')

            lg_process.info('finish process whith '
                            'open interamerican university succesfuly.')

        u_nacional_la_pampa()
        open_interamerican_u()

    # function to send data process to s3
    def send():
        lg_send.info('initializing DAG send data.')

    # pythonoperator for function of connect and queries
    python_connect = PythonOperator(
        task_id="connect_queries",
        python_callable=conn_query,
        retries=5,
    )

    # pythonoperator for function to process data
    python_process = PythonOperator(
        task_id="process",
        python_callable=proccess,
    )

    # pythonoperator for function to send data to s3
    python_send = PythonOperator(
        task_id="send_to_up_cloud",
        python_callable=send,
    )

    python_connect >> python_process >> python_send
