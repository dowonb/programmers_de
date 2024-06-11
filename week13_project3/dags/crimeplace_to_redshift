'''
* KOSIS API 사용 DAG 작성_v2*
+ category 추가
+ 계 삭제
'''


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import logging
import requests

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_crime_info():
    response = requests.get('https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=MGU3MGE4ZWU1YjYzNmJiNjFjMTEzODNkMGE0YmZiYTI=&itmId=00+&objL1=00+01+0101+0102+0103+0104+0106+0107+0108+0109+0105+02+03+0301+0302+0303+0304+0305+0306+0307+0308+07+10+&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&newEstPrdCnt=10&orgId=132&tblId=DT_13204_3106')
    crimes = response.json()
    records = []
    for crime in crimes:
        occured_date = crime["PRD_DE"]
        subcategory = crime["C1"]
        if subcategory in ['00', '01', '03']:
            continue
        subcategory_nm = crime["C1_NM"]
        category = {"01": "강력범죄", "02" : "절도범죄","03" : "폭력범죄", "07": "마약범죄", "10" : "교통범죄"}.get(subcategory[0:2])
        place = crime["C2_NM"]
        if place == '계':
            continue
        place_cnt0 = crime["DT"]
        place_cnt = place_cnt0.replace("-", "0")
        records.append([occured_date, category, subcategory, subcategory_nm, place, place_cnt])
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
    occured_date int,
    category  VARCHAR(20),
    subcategory int,
    subcategory_nm VARCHAR(30),
    place  VARCHAR(30),
    place_cnt int
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ({r[0]}, '{r[1]}', {r[2]}, '{r[3]}', '{r[4]}', {r[5]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'CrimePlaceToRedshift',
    start_date = datetime(2023,1,1),
    catchup=False,
    tags=['API'],
    schedule = '@yearly' # 	1년에 한번씩 1월 1일 자정에 실행
) as dag:

    results = get_crime_info()
    load("dowon0215", "crime_place_v2", results)
