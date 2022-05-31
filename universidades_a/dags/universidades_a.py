from datetime import date, datetime
from pathlib import Path

import pandas as pd


def age(born):
    '''
    Function that receives a date and returns the current age

    Returns:
                    (int): current age
    '''
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    return today.year - born.year - (
        (today.month, today.day)
        < (born.month, born.day))


def transform_flores():
    '''
    Function that will normalize the data of the Universidad de Villa Maria.
    Raw data is extracted from the corresponding csv file and through
    Pandas that information is processed as required.

    Returns:
                    (str): normalization result message
    '''
    try:
        filepath_flores_csv = Path(
            'airflow/universidades_a/dags/files/universidad_flores.csv')
        filepath_cpaloc_csv = Path(
            'airflow/universidades_a/dags/files/codigos_postales.csv')
        df_cpaloc = pd.read_csv(filepath_cpaloc_csv, sep=',')
        df_flores = pd.read_csv(filepath_flores_csv, sep=',')
        # column direccion not required for processing
        df_flores = df_flores.drop('direccion', axis=1)
        # location is added to the table by doing a merge
        # through the postal code
        df_flores = df_flores.merge(df_cpaloc, on='codigo_postal')
        # the corresponding columns are modified to lowercase
        # and the spaces are eliminated
        columns_tolower = [
            'universidad', 'carrera', 'name',
            'correo_electronico', 'localidad']
        for i in range(len(columns_tolower)):
            df_flores[columns_tolower[i]] = df_flores[
                columns_tolower[i]].str.lower()
            df_flores[columns_tolower[i]] = df_flores[
                columns_tolower[i]].str.strip(' ')
        df_flores['sexo'] = df_flores['sexo'].replace(
            ['F', 'M'], ['female', 'male'], regex=True)
        # the suffixes are eliminated from the name column and
        # then they are separated by first and last name,
        # only 1 last name is taken
        df_flores['name'] = df_flores['name'].str.lstrip('mrs. ')
        df_flores['name'] = df_flores['name'].str.lstrip('ms. ')
        df_flores['name'] = df_flores['name'].str.lstrip('mr. ')
        name = df_flores['name'].str.split(expand=True)
        name = name.drop([2, 3], axis=1)
        name.columns = ['first_name', 'last_name']
        df_flores = pd.concat([df_flores, name], axis=1)
        df_flores = df_flores.drop('name', axis=1)
        # age is calculated through the Age function
        df_flores['age'] = df_flores['fecha_nacimiento'].apply(age)
        df_flores = df_flores.drop('fecha_nacimiento', axis=1)
        # columns are renamed and ordered as required
        df_flores = df_flores.set_axis(
            ['university', 'career', 'inscription_date', 'gender',
                'postal_code', 'email', 'location', 'first_name',
                'last_name', 'age'], axis=1)
        df_flores = df_flores.reindex(columns=[
                    'university', 'career', 'inscription_date',
                    'first_name', 'last_name', 'gender', 'age',
                    'postal_code', 'location', 'email'])
        # the type of postal code is modified as required
        df_flores['postal_code'] = df_flores['postal_code'].astype('object')
        # directory is generated and the file is saved
        # with the normalizations
        filepath_flores_txt = Path(
            'airflow/universidades_a/dags/files/universidad_flores.txt')
        filepath_flores_txt.parent.mkdir(parents=True, exist_ok=True)
        df_flores.to_csv(filepath_flores_txt, index=False)
    except Exception as exc:
        return exc
    finally:
        return "success"


def transform_villamaria():
    '''
    Function that will normalize the data of the Universidad de Villa Maria.
    Raw data is extracted from the corresponding csv file and through
    Pandas that information is processed as required.

    Returns:
                    (str): normalization result message
    '''
    try:
        filepath_villamaria_csv = Path(
            'airflow/universidades_a/dags/files/universidad_villamaria.csv')
        filepath_cpaloc_csv = Path(
            'airflow/universidades_a/dags/files/codigos_postales.csv')
        df_cpaloc = pd.read_csv(filepath_cpaloc_csv, sep=',')
        # puplicate locations that refer to more than one postl code should
        # be eliminated for not having more information regarding the location
        df_cpaloc = df_cpaloc.drop_duplicates(subset=['localidad'])
        df_villamaria = pd.read_csv(filepath_villamaria_csv, sep=',')
        # column direccion not required for processing
        df_villamaria = df_villamaria.drop('direccion', axis=1)
        # the corresponding columns are modified underscore to
        # spaces as required
        column_to_normalize = ['universidad', 'carrera', 'nombre', 'localidad']
        for i in range(len(column_to_normalize)):
            df_villamaria[column_to_normalize[i]] = df_villamaria[
                column_to_normalize[i]].replace('_', ' ', regex=True)
        # postal_code is added to the table by doing a merge
        # through the location
        df_villamaria = df_villamaria.merge(df_cpaloc, on='localidad')
        # the corresponding columns are modified to lowercase
        # and the spaces are eliminated
        columns_tolower = [
            'universidad', 'carrera', 'nombre', 'email', 'localidad']
        for i in range(len(columns_tolower)):
            df_villamaria[columns_tolower[i]] = df_villamaria[
                columns_tolower[i]].str.lower()
            df_villamaria[columns_tolower[i]] = df_villamaria[
                columns_tolower[i]].str.strip(' ')
        # format of the dates is modified as required
        df_villamaria['fecha_de_inscripcion'] = pd.to_datetime(
            df_villamaria['fecha_de_inscripcion']).dt.strftime('%Y-%m-%d')
        df_villamaria['fecha_nacimiento'] = pd.to_datetime(
            df_villamaria['fecha_nacimiento']).dt.strftime('%Y-%m-%d')
        # age is calculated through the Age function
        df_villamaria['age'] = df_villamaria['fecha_nacimiento'].apply(age)
        df_villamaria = df_villamaria.drop('fecha_nacimiento', axis=1)
        df_villamaria['sexo'] = df_villamaria['sexo'].replace(
            ['F', 'M'], ['female', 'male'], regex=True)
        # the suffixes are eliminated from the name column and
        # then they are separated by first and last name,
        # only 1 last name is taken
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('mrs. ')
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('ms. ')
        df_villamaria['nombre'] = df_villamaria['nombre'].str.lstrip('mr. ')
        name = df_villamaria['nombre'].str.split(expand=True)
        name = name.drop([2, 3], axis=1)
        name.columns = ['first_name', 'last_name']
        df_villamaria = pd.concat([df_villamaria, name], axis=1)
        df_villamaria = df_villamaria.drop('nombre', axis=1)
        # columns are renamed and ordered as required
        df_villamaria = df_villamaria.set_axis(
            ['university', 'career', 'inscription_date', 'gender',
                'location', 'email', 'postal_code', 'age', 'first_name',
                'last_name'], axis=1)
        df_villamaria = df_villamaria.reindex(columns=[
                    'university', 'career', 'inscription_date',
                    'first_name', 'last_name', 'gender', 'age',
                    'postal_code', 'location', 'email'])
        # the type of postal code is modified as required
        df_villamaria['postal_code'] = df_villamaria[
            'postal_code'] .astype('object')
        # directory is generated and the file is saved
        # with the normalizations
        filepath_villamaria_txt = Path(
            'airflow/universidades_a/dags/files/universidad_villamaria.txt')
        filepath_villamaria_txt.parent.mkdir(parents=True, exist_ok=True)
        df_villamaria.to_csv(filepath_villamaria_txt, index=False)
    except Exception as exc:
        return exc
    finally:
        return "success"
