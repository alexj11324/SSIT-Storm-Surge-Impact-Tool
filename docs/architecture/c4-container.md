> **Historical**: This document reflects the pre-Colab-migration cloud architecture. Current pipeline runs locally with DuckDB + Google Colab.

# C4 Container-Level Documentation

## 1. Containers Section

### 1. Main Execution Container: AWS EC2 Spot Node
- **Description**: A parallelized Unix compute boundary running Python 3.10 with Anaconda environment containing `duckdb`, `pyarrow`, `geopandas`, and `fast_engine`.
- **Technology**: Python, Bash, Ubuntu/Linux, AWS Boto3.
- **Deployment**: Invoked via CLI remotely. `launch_cloud_parallel.sh` automatically provisions instances, clones repo, prepares Anaconda environments, unloads payload, and terminates via `terminate_parallel.sh`.

### 2. Analytical Storage Container: AWS S3 + Athena
- **Description**: Object storage system decoupled from compute. Raw files (NSI and SLOSH rasters) lie inside buckets.
- **Technology**: S3, Parquet columnar format, AWS Athena query engine.
- **Deployment**: Persistent data blobs.

## 2. Component Layout inside Container

**Within AWS EC2 Node:**
- Pipeline Orchestrator Component
- FAST Engine Wrapper Component
- Configuration State Component

## 3. Interfaces Section
*(As this is an ephemeral data pipeline, there is no persistent REST/GraphQL API serving HTTP. It executes stateless tasks.)*
- **S3 Bucket Interface**: Requires `pyarrow.fs.S3FileSystem` and AWS programmatic access keys to list and write `part-xxx.parquet` structures.

## 4. Dependencies
- Submits results back to **AWS Athena** (querying over s3 parquet buckets using standard ANSI SQL).

## 5. Container Diagram

```mermaid
C4Container
    title Container diagram for ARC Capstone Geospatial Pipeline

    Person(data_scientist, "Data Scientist", "Triggers bash script locally or via CI")
    
    System_Boundary(aws_boundary, "AWS Cloud Infrastructure") {
        Container(ec2_spot, "Batch EC2 Spot Instances", "Python 3.10", "Executes DuckDB spatial merges and FEMA FAST algorithms")
        ContainerDb(s3_storage, "S3 Object Storage", "Parquet", "Stores 30M+ NSI Buildings and SLOSH TIF grids")
        ContainerDb(athena, "AWS Athena", "Presto/SQL", "Distributed querying of generated predictions")
    }
    
    SystemExt(fema, "FEMA FAST Tool", "Proprietary physical destruction formulas")
    
    Rel(data_scientist, ec2_spot, "Invokes run via launch_cloud_parallel.sh", "SSH/Bash")
    Rel(ec2_spot, s3_storage, "Reads NSI base & Stores generated Predictions", "HTTPS/S3 API")
    Rel(ec2_spot, fema, "Passes structures to sub-shell via CSV", "Local OS Shell")
    Rel(athena, s3_storage, "Queries partition folders", "Internal AWS")
```
