# I22-2594_MLOps_Assignment-3-NASA_APOD_Data_Pipeline
# NASA APOD Data Pipeline

This project implements an MLOps pipeline for NASA Astronomy Picture of the Day (APOD) data using Apache Airflow, Docker, and DVC.

## Architecture

The pipeline includes:

1. **Data Extraction**: Retrieve APOD data from NASA API
2. **Data Transformation**: Clean and process the data
3. **Data Loading**: Save to PostgreSQL and CSV
4. **Data Versioning**: Use DVC for data versioning
5. **Code Versioning**: Commit to Git

## Prerequisites

- Docker
- Docker Compose
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd I22-2594_MLOps_Assignment-3-NASA_APOD_Data_Pipeline
```

### 2. Build and Start the Services

```bash
docker compose up --build -d
```

### 3. Access the Airflow UI

Open your browser and navigate to: `http://localhost:8080`

Default credentials:
- Username: `admin`
- Password: `admin`

### 4. Check the DAG

- In Airflow UI, you should see the `nasa_apod_pipeline` DAG
- You can trigger it manually or wait for the daily schedule

## Pipeline Overview

The pipeline (`dags/main.py`) includes the following tasks:

- `extract_data`: Fetches data from NASA APOD API
- `transform_data`: Cleans and transforms the data
- `load_to_postgres`: Loads data to PostgreSQL database
- `save_to_csv`: Saves data to CSV file
- `version_with_dvc`: Versions the CSV file with DVC
- `commit_to_git`: Commits changes to Git

## Services

- **Airflow Webserver**: `http://localhost:8080`
- **Airflow Scheduler**: Runs the scheduled DAGs
- **PostgreSQL**: `localhost:5433` (user: `airflow`, password: `airflow`, db: `airflow`)

## Data Storage

- **PostgreSQL**: Persistent data storage
- **CSV files**: Saved in `./data/` directory
- **DVC**: Data versioning for CSV files

## Troubleshooting

If you encounter any issues:

1. Check container status:
   ```bash
   docker compose ps
   ```

2. Check logs for a specific service:
   ```bash
   docker compose logs <service-name>
   ```

3. If DAG doesn't appear in Airflow UI, restart the webserver:
   ```bash
   docker compose restart airflow-webserver
   ```

## Stopping the Services

To stop the services:

```bash
docker compose down
```

To stop and remove containers, networks, and volumes:

```bash
docker compose down -v
```

## Dockerfile Changes

The Dockerfile was updated to run pip commands as the airflow user instead of root to comply with Airflow security requirements.

## Data Pipeline Flow

1. **Extract**: Daily, the pipeline extracts APOD data from NASA API
2. **Transform**: Transforms data and cleans it
3. **Load**: Loads data to PostgreSQL and CSV file
4. **Version**: Uses DVC to version the CSV file
5. **Commit**: Commits the DVC files to Git
