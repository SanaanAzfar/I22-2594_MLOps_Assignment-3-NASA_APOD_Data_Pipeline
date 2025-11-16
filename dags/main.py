from datetime import datetime
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),  
}

def transform_data(**kwargs):
    """Step 2: Transform and clean the data"""
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data')
    
    selected_fields = {
        'date': raw_data.get('date'),
        'title': raw_data.get('title'),
        'url': raw_data.get('url'),
        'explanation': raw_data.get('explanation'),
        'media_type': raw_data.get('media_type'),
        'service_version': raw_data.get('service_version')
    }
    
    df = pd.DataFrame([selected_fields])
    df['date'] = pd.to_datetime(df['date'])
    df['title'] = df['title'].str.strip()
    
    print(f"Transformed data: {len(df)} records")
    return df



def extract_data():
    print("extracting NASA APOD data")
    url = "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY"

    try:
        response = requests.get(url) 
        response.raise_for_status()
        data = response.json()
        print(f"Extracted Data: {data['title']}")  
        return data
    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        raise

with DAG(
    'nasa_apod_pipeline',  
    default_args=default_args,
    schedule='@daily', 
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_apod_data
)

extract_task >> transform_task
