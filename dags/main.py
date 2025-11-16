from datetime import datetime
import psycopg2
import os
import requests
import pandas as pd
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

def commit_to_git(**kwargs):
    """Step 5: Commit DVC metadata to Git"""
    try:
        
        subprocess.run(['git', 'config', 'user.email', 'sanaanazfar742004@example.com'])
        subprocess.run(['git', 'config', 'user.name', 'SanaanAzfar'])
        
    
        subprocess.run(['git', 'add', 'apod_data.csv.dvc', '.gitignore'], check=True)
        
        commit_result = subprocess.run(
            ['git', 'commit', '-m', f'Update APOD data: {datetime.now().isoformat()}'],
            capture_output=True,
            text=True
        )
        
        if commit_result.returncode == 0:
            print(" Git commit successful")
            return True
        else:
            print(f" Git commit: {commit_result.stderr}")
            return True  
            
    except Exception as e:
        print(f" Git operation: {str(e)}")
        return True  


def version_with_dvc(**kwargs):
    try:
        result = subprocess.run(
            ['dvc', 'add', 'data/apod_data.csv'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(" DVC versioning successful")
            return True
        else:
            print(f" DVC failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f" DVC error: {str(e)}")
        return False

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),  
}

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data')
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow", 
        user="airflow",
        password="airflow",
        port="5432"
    )
    
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nasa_apod (
                date DATE PRIMARY KEY,
                title TEXT,
                url TEXT,
                explanation TEXT,
                media_type VARCHAR(50),
                service_version VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO nasa_apod (date, title, url, explanation, media_type, service_version)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    explanation = EXCLUDED.explanation;
            """, (row['date'], row['title'], row['url'], row['explanation'], row['media_type'], row['service_version']))
    
    conn.commit()
    conn.close()
    print("Data loaded to PostgreSQL")


def save_to_csv(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data')
    
    csv_path = "/data/apod_data.csv"
    
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_path, index=False)
    
    print(" Data saved to CSV")



def transform_data(**kwargs):
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
        python_callable=transform_data
    )

    load_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    save_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv
    )

    extract_task >> transform_task >> [load_postgres_task, save_csv_task]

    dvc_task = PythonOperator(
    task_id='version_with_dvc',
    python_callable=version_with_dvc
    )

    [load_postgres_task, save_csv_task] >> dvc_task

    git_task = PythonOperator(
    task_id='commit_to_git',
    python_callable=commit_to_git
    )

    dvc_task >> git_task
