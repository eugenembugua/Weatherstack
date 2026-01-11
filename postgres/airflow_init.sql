-- 1. Create the database for Airflow Metadata
CREATE DATABASE airflow;

-- 2. Connect to weatherstack to set up your data layers
\c weatherstack;

CREATE SCHEMA IF NOT EXISTS data;

-- 3. Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA data TO eugene;
GRANT ALL ON ALL TABLES IN SCHEMA data TO eugene;
ALTER DEFAULT PRIVILEGES IN SCHEMA data GRANT ALL ON TABLES TO eugene;