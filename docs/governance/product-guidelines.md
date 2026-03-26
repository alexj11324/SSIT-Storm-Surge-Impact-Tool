# Product Guidelines

## Voice and Tone
**Professional and technical**
Communication in logs, documentation, output schemas, and interfaces should be explicit, unambiguous, and data-driven without unnecessary conversational filler. Use strict technical terminology corresponding to disaster research and software engineering.

## Design Principles
**Performance first**
We are processing large-scale geospatial structural datasets (e.g., 30M+ buildings nationwide from NSI, plus dense SLOSH grid files). Every module, data operation, and database choice (like DuckDB and Parquet over heavy SQL layers) must maximize processing efficiency, lower memory footprint, and parallelize effectively. Wait times block disaster response, so speed is critical.
