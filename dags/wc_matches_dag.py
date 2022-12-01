import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import psycopg2 as pg
import glob
from pathlib import Path
import csv
import numpy as np

def get_matches():
    web = f'https://en.wikipedia.org/wiki/2022_FIFA_World_Cup'
    response = requests.get(web)
    content = response.text
    soup = BeautifulSoup(content, 'lxml')
    matches = soup.find_all('div', class_='footballbox')

    home = []
    score = []
    away = []

    for match in matches:
        home.append(match.find('th', class_='fhome').get_text())
        score.append(match.find('th', class_='fscore').get_text())
        away.append(match.find('th', class_='faway').get_text())

    dict_football = {'home': home, 'score': score, 'away': away}
    df_football = pd.DataFrame(dict_football)
    df_football['year'] = 2022
    return df_football

csv_path = Path("/opt/airflow/data/wc_matches_2022_crawl.csv")

def transform_data(**kwargs):
    # Xcoms to get the list
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='scraping_data')
    df_football = pd.DataFrame(value)
    df1_football = df_football.dropna()
    df1_football['home'] = df1_football['home'].str.strip()
    df1_football['away'] = df1_football['away'].str.strip()

    # cleaning score
    df1_football.loc[df1_football['score'].str.contains('Match'), 'score'] = '0–0'
    df1_football['score'] = df1_football['score'].str.replace('[^\d–]', '', regex=True)

    # splitting score columns into home and away goals and dropping score column
    df1_football[['HomeGoals', 'AwayGoals']] = df1_football['score'].str.split('–', expand=True)
    df1_football.drop('score', axis=1, inplace=True)
    
    # renaming columns and changing data types
    df1_football.rename(columns={'home': 'HomeTeam', 'away': 'AwayTeam', 'year':'Year'}, inplace=True)
    df1_football['HomeGoals'] = df1_football['HomeGoals'].fillna(0)
    df1_football['AwayGoals'] = df1_football['AwayGoals'].fillna(0)
    df1_football['Year'] = df1_football['Year'].fillna(0)
    df1_football['HomeGoals'] = df1_football['HomeGoals'].astype(int)
    df1_football['AwayGoals'] = df1_football['AwayGoals'].astype(int)
    df1_football['Year'] = df1_football['Year'].astype(int)

    # cleaning rows
    df1_football = df1_football[~df1_football['HomeTeam'].str.contains('Winners|Losers')]

    # creating new column "totalgoals"
    df1_football['TotalGoals'] = df1_football['HomeGoals'] + df1_football['AwayGoals']
    df1_football
    try:
        print(df1_football)
        df1_football.to_csv(csv_path, index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False

def load_to_postgres():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='airflow-docker-postgres-1' password='airflow'"
        )
    except Exception as error:
        print(error)
    path = "/opt/airflow/data/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
        fname = fname.split('/')
        csvname = fname[-1]
        csvname = csvname.split('.')
        tablename = str(csvname[0])
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fix_wc_matches_2022_""" + tablename + """ (
        HomeTeam varchar(50),
        AwayTeam varchar(50),
        Year varchar(50),
        HomeGoals varchar(50),
        AwayGoals varchar(50),
        TotalGoals varchar(50)
        );
        """
    )
    conn.commit()

    with open('/opt/airflow/data/wc_matches_2022_crawl.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO fix_wc_matches_2022_wc_matches_2022_crawl VALUES (%s, %s, %s, %s, %s, %s)",
                row
            )
    conn.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'wc_matches_dag',
    default_args=default_args,
    description='DAG for World Cup 2022 Matches',
    schedule_interval=timedelta(hours=12),
)

t1 = PythonOperator(
    task_id='scraping_data',
    python_callable=get_matches,
    # provide_context=True,
    dag=dag)
t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag)
t3 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag)

t1 >>  t2 >> t3