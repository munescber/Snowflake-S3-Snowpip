# Snowflake Snowpipe Demo

This repository contains SQL scripts that demonstrate how to build an automated data ingestion pipeline in Snowflake using **Snowpipe**, **external stages**, and **S3 event notifications**.

## 📁 Contents

- **STAGECREATIONFORSNOWPIPE.SQL**  
  Creates the target table, file format, storage integration, and external stage used for Snowpipe ingestion.

- **PIPE_CREATION.SQL**  
  Defines a Snowpipe that automatically loads CSV files from an S3 bucket into Snowflake.

- **MODIFY_PIPE.SQL**  
  Shows how to pause, modify, refresh, and resume an existing Snowpipe, including redirecting it to a new table.

## 🚀 What This Project Demonstrates

- Creating Snowflake tables and file formats  
- Connecting Snowflake to AWS S3 using a storage integration  
- Building an external stage for file ingestion  
- Creating and managing a Snowpipe with auto-ingest  
- Refreshing and modifying existing pipes  
- Previewing staged files and validating loads  

## 🧩 Requirements

- Snowflake account  
- AWS S3 bucket  
- IAM role configured for Snowflake external access  

## 📦 How It Works

1. Files are uploaded to an S3 bucket.  
2. S3 event notifications trigger Snowpipe.  
3. Snowpipe loads new files into Snowflake automatically.  
4. Load history can be monitored using Snowflake views.

## 📜 License

This project is provided for learning and demonstration purposes.
