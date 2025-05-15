# NYC Taxi Data Pipeline 🚖

A modern, modular, and cloud-integrated data pipeline that ingests, transforms, and persists New York City taxi trip data using industry-grade tools such as Kestra, DuckDB, PostgreSQL, and MotherDuck. This project demonstrates real-world data engineering patterns, leveraging open-source orchestration, embedded analytics, and hybrid cloud/local database architectures.

## 🧠 Project Overview

This pipeline automates the entire lifecycle of data—from raw ingestion to structured storage and analysis-ready transformations—by orchestrating the following:

* Ingestion of Parquet files from the NYC TLC public dataset
* Schema discovery and transformation using DuckDB
* Dual persistence: local PostgreSQL (staging and warehouse) and cloud-based MotherDuck
* Workflow automation and monitoring via Kestra

Built for clarity, reproducibility, and extensibility, this system demonstrates how cloud-native and local compute can seamlessly cooperate to process real-world data.

---

## 🛠️ Technologies Used

| Component        | Role                                               |
| ---------------- | -------------------------------------------------- |
| 🛠️ Kestra       | Orchestrates the end-to-end pipeline               |
| 🦆 DuckDB        | Handles fast, in-memory SQL and Parquet processing |
| 🐘 PostgreSQL    | Staging and warehouse layers (Dockerized)          |
| ☁️ MotherDuck    | Cloud-optimized DuckDB for storage and analytics   |
| 🐳 Docker        | Containerized environment for local dev            |
| 📊 NYC Taxi Data | Real-world Parquet datasets for ingestion          |

---

## 📥 Features

* ⛓️ End-to-end workflow automation using Kestra
* 🔁 Incremental ingestion of large-scale Parquet files
* 🔍 Dynamic schema inference using DuckDB
* 🧰 SQL-based transformations with compatibility across DuckDB/Postgres
* 🌐 Hybrid storage: Cloud-first MotherDuck + local PostgreSQL
* 📦 Container-first setup for reproducible pipelines

---

## 🚀 Getting Started

### Prerequisites

* Docker & Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/slackeddoodler/Bridging-Data-with-Workflow-Orchestration--NYC-Taxi-Pipeline-Using-Kestra-DuckDB.git
cd Bridging-Data-with-Workflow-Orchestration--NYC-Taxi-Pipeline-Using-Kestra-DuckDB
```

### 2. Create a Docker Network

```
docker network create kestras-net
```

### 3. Start PostgreSQL and Kestra

Navigate to the appropriate folder and run the following commands to start PostgreSQL and Kestra:

Start PostgreSQL:

```
cd postgres
docker-compose up -d
```

Start Kestra:

```
cd kestra
docker-compose up -d
```

### 4. Access Kestra UI

Once both services are running, open your browser and navigate to:

```
http://localhost:8080
```

Use the Kestra web interface to:

* Deploy and trigger workflows
* Monitor execution logs
* Inspect task outputs and durations

---

## 🛠️ Workflow Overview

### 1. **`postgres_taxi` Workflow**

This manual workflow handles the ingestion of Parquet files, schema creation using DuckDB, inserting the data into the staging table, and finally transferring it into the main table in Postgres Database.

### 2. **`postgres_scheduled` Workflow (with Backfill)**

This scheduled workflow, using Kestra's backfill feature, handles the ingestion of both new and historical data into Postgres Database. It ensures that when new data is available or a gap in data occurs, the process runs automatically without requiring manual intervention.

### 3. **`motherduck_taxi` Workflow**

This manual workflow handles the ingestion of Parquet files, schema creation using DuckDB, inserting the data into the staging table, and finally transferring it into the main table in MotherDuck. By shifting compute to the cloud, this approach mitigates local resource constraints and enhances processing efficiency.

### 4. **`motherduck_scheduled` Workflow (with Backfill)**

This scheduled workflow, using Kestra's backfill feature, handles the ingestion of both new and historical data into MotherDuck. It ensures that when new data is available or a gap in data occurs, the process runs automatically without requiring manual intervention.


---

## ⚙️ MotherDuck Integration Notes

For using the **MotherDuck** workflows, the following steps are required:

1. **Create a MotherDuck Account**:
   Sign up for an account at [MotherDuck](https://www.motherduck.com) and generate an **Access Token**.

2. **Create a Database**:
   Create a new database within your MotherDuck account and replace `nyc_taxi` with your database name in all the instances of the `url` property in both workflows.
   
   For example, replace:
   ```
   - id: yellow_create_staging_table
        type: io.kestra.plugin.jdbc.duckdb.Query
        url: "jdbc:duckdb:md:nyc_taxi?motherduck_token={{ secret('MOTHERDUCK_TOKEN') }}"
   ```
   with:
   ```
   - id: yellow_create_staging_table
        type: io.kestra.plugin.jdbc.duckdb.Query
        url: "jdbc:duckdb:md:your_database_name?motherduck_token={{ secret('MOTHERDUCK_TOKEN') }}"
   ```
   Or you can create a database with the name `nyc_taxi`.

2. **Create a `.env-encoded` File**:
   Inside the folder that contains the **Kestra Docker Compose YAML file**, create a `.env-encoded` file and add the following line:

   ```
   SECRET_MOTHERDUCK_TOKEN=youraccesstoken
   ```

   * Replace `youraccesstoken` with the access token you received from MotherDuck.

3. **Base64 Encode the Token**:
   You need to convert the token into a base64 format.
   Run the following command in your terminal (Bash must be installed):

   ```bash
   echo -n "youraccesstoken" | base64
   ```

4. **Update `.env-encoded` File with Base64-encoded Token**:
   Copy the base64 output and paste it into the `.env-encoded` file under the `SECRET_MOTHERDUCK_TOKEN` variable:

   ```
   SECRET_MOTHERDUCK_TOKEN=base64encodedtoken
   ```

   **Important**: Ensure the `SECRET_` prefix is included when defining the token in the `.env-encoded` file.

5. **Reference the `.env-encoded` file in docker-compose.yml**
  In the Kestra docker-compose.yml file, add the `.env-encoded` directive under the Kestra service definition:
    ```
    kestra:
      image: kestra/kestra:latest
      env_file:
        - .env-encoded
    ```

7. **Restart Docker Containers**:
   After adding the `.env-encoded` file to the yml file, restart your Docker containers to apply the changes:

   ```
   docker-compose down
   docker-compose up -d
   ```

---

## 📈 Example Output

* Tables Created:

  * green\_tripdata
  * green\_tripdata\_staging
  * yellow\_tripdata
  * yellow\_tripdata\_staging
* Data Volume:

  * 20+ million rows processed in under 2 minutes using MotherDuck
* Compatible SQL:

  * Queryable in both DuckDB (MotherDuck) and PostgreSQL

---

## 🔒 Security & Privacy

* All processing is local unless explicitly sent to MotherDuck.
* No personally identifiable information (PII) is handled.
* Docker isolation ensures reproducibility and sandboxing.

---

## 🎯 Why This Project Matters

This project is a blueprint for modern data engineering:

* Demonstrates ETL patterns with zero third-party vendor lock-in.
* Bridges local compute and cloud-native analytical storage.
* Showcases orchestration-first mindset with declarative workflows.
