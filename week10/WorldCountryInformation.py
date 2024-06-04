'''
*세계 나라 정보 API 사용 DAG 작성*
- https://restcountries.com/v3/all를 호출하여 나라별로 다양한 정보를 얻을 수 있음 (별도의 API Key가 필요없음)
- Full Refresh로 구현해서 매번 국가 정보를 읽어오게 할 것!
- API 결과에서 아래 3개의 정보를 추출하여 Redshift에 각자 스키마 밑에 테이블 생성
  - country -> [“name”][“official”]
  - population -> [“population”]
  - area -> [“area”]
- 단 이 DAG는 UTC로 매주 토요일 오전 6시 30분에 실행되게 만들어볼 것!
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
import requests
import time

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_country_info():
    response = requests.get('https://restcountries.com/v3/all', verify=False)
    countries = response.json()
    records = []
    for country in countries:
        name1 = country["name"]["official"]
        name = name1.replace("'", "")
        population = country["population"]
        area = country["area"]
        records.append([name, population, area])
    return records

@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    name varchar(80),
    population int,
    area int
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'WorldCountryInformation',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6' # 토요일 6:30 실행
) as dag:

    results = get_country_info()
    load("dowon0215", "world_county_info", results)
