# Principal-Level Architecture Guide

Welcome to the Red Cross ARC Capstone codebase. This guide is heavily opinionated and designed for senior engineers who need to grasp the core mechanics immediately.

## The Core Architectural Insight

The core insight of this system is **converting high-precision geoinformatics algorithms into parallelizable, in-process, distributed stateless operators**. 
Instead of loading 30 million NSI structures into a massive PostGIS/Oracle SQL database which forms a harsh bottleneck for spatial joins, this project aggressively uses **Parquet files on S3 + DuckDB in-memory aggregations + Headless FEMA FAST**.

If I had to express the entire project's operational mentality in pseudocode (using standard Spark/Scala paradigm as comparison):

```scala
// Traditional approach: bottlenecked by DB transactions and complex joins
val dbData = PostGISDatabase.load("NSI_Houses_Grid")
val results = dbData.spatialJoin(SLOSH_Polygons).map(predictDamage).saveToSalesforce()

// Our Insight (DuckDB + Parquet)
val nsiParquets = S3.list("bucket/nsi/*.parquet")
// Execute in parallel nodes directly touching blob storage via duckdb
parallelExecution(nsiParquets).map( file => {
   val duckDbCtx = DuckDB.memory() // Zero overhead spin-up
   val fastInput = duckDbCtx.sql(s"SELECT * FROM '$file' JOIN ST_MakeValid(...)")
   val damage = FEMA_FAST_Headless.run(fastInput)
   S3.write(damage, "bucket/predictions/part-X.parquet")
})
// Combine/query globally via Athena
```

## Abstract Architecture and Domain Model

```mermaid
graph TD
    A[S3: NSI Base Building Data\nPartitioned Parquet] --> C{DuckDB\nIn-Process Spatial Join}
    B[S3: NOAA SLOSH Models\nTiff Rasters] --> C
    C --> |FAST format CSV| D[FEMA FAST Engine\n(Headless Python)]
    D --> E[S3 Output Predictions\nParquet/CSV]
    E --> F[AWS Athena Query Engine]
    F --> G[Excel/Red Cross Dashboard]
    
    classDef storage fill:#ff9900,stroke:#fff,stroke-width:2px,color:#000;
    classDef compute fill:#1f77b4,stroke:#fff,stroke-width:2px;
    class A,B,E,F storage;
    class C,D compute;
```

### Design Tradeoffs

1. **Parquet/DuckDB vs Relational DB (PostgreSQL/Oracle)**
   - *Why*: Relational databases present a bottleneck for rapid, mass-scale read/write during disaster zero-hour response loops. DuckDB executes OLAP queries extremely fast directly on S3 blobs.
   - *Tradeoff*: You lose granular single-row transactional features, but disaster evaluation is inherently a mass-batch operation.

2. **Decoupled Shell Execution vs Heavy Orchestrator (Airflow)**
   - *Why*: The system requires lightweight scripts (`launch_cloud_parallel.sh`, `monitor_parallel.sh`) to trigger massive EC2 spots right before the storm hits without maintaining persistent orchestration servers that eat budget.
   - *Tradeoff*: Lacks fine-grained UI tracking of individual worker drops.

## Where to Go Deep
If you only look at two scripts to understand the entire logic:
- `scripts/duckdb_fast_pipeline.py`: The heart of the extraction and formatting phase logic, moving massive data arrays.
- `scripts/match_county_coverage_cloud.py`: To see how geographical subsetting and bounds validation work.
