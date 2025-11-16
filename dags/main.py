from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator  

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),  
}

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
