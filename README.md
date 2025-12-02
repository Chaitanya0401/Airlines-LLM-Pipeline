Aviation ETL & LLM Insights Pipeline

Overview:
An end-to-end aviation ETL pipeline that fetches flight data, transforms it, and generates insightful reports using Airflow, AWS S3, and LLMs.

Key Features
1. Data Ingestion

Fetches live flight data from AviationStack API.

Stores raw JSON files in AWS S3 (raw/ folder).

2. Data Transformation

Cleans and structures flight data: airlines, airports, flight status.

Saves processed data as CSV and JSON in S3 (processed/ folder).

Orchestrated using Airflow DAGs (ingest → transform → report).

3. LLM-Powered Reports

Uses ChatOllama / Llama2 to generate aviation summaries.

Reports include:

Total number of flights

Top departure/arrival airports

Airline distribution

Flight status breakdown

Patterns and anomalies

Reports stored in S3 (reports/ folder).

4. Local / Cloud Storage

S3 is the central storage for raw, processed, and report data.

Can be run fully locally (Docker + Airflow) or connected to AWS S3.
