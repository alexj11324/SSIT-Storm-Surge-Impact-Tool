# Wiki Index / Content Catalogue

```json
{
  "name": "arc-capstone-wiki",
  "title": "ARC Capstone Project Wiki",
  "items": [
    {
      "name": "onboarding",
      "title": "Onboarding & Fast Track",
      "children": [
        {
          "name": "principal-guide",
          "title": "Principal-Level Architecture Guide",
          "prompt": "Read docs/wiki/principal_guide.md to understand the core architectural insight, critical tradeoffs between DuckDB/Parquet vs SQL, and where to step in."
        },
        {
          "name": "zero-to-hero",
          "title": "Zero-to-Hero Learning Path",
          "prompt": "Read docs/wiki/zero_to_hero.md for foundational context converting NSI and SLOSH formats, navigating this codebase, and running your first batch pipeline."
        }
      ]
    },
    {
      "name": "getting-started",
      "title": "Getting Started",
      "children": [
        {
          "name": "setup",
          "title": "Local & Cloud Environment Setup",
          "prompt": "See README.md for Prerequisites (`pip install pyarrow rasterio...`) and Cloud setup using script references like `scripts/bootstrap_cloud_fast_env.sh`."
        },
        {
          "name": "usage",
          "title": "Executing the Pipelines",
          "prompt": "See README.md 'Quick Start' on using `scripts/fast_e2e_from_oracle.py` and `scripts/duckdb_fast_pipeline.py`."
        }
      ]
    },
    {
      "name": "deep-dive",
      "title": "Deep Dive: System Architecture",
      "children": [
        {
          "name": "data-pipeline",
          "title": "Data Pipeline and Transformations",
          "prompt": "Understand how SLOSH outputs rasterize via `scripts/slosh_to_raster.py` and merge with Parquet data in `scripts/fast_e2e_from_oracle.py`."
        },
        {
          "name": "fast-engine",
          "title": "FEMA FAST Engine Execution",
          "prompt": "Review integration logic with `FAST-main/Python_env/run_fast.py` to produce building damage metrics (`BldgDmgPct`, `BldgLossUSD`)."
        },
        {
          "name": "cloud-infrastructure",
          "title": "AWS / OCI Cloud Operations",
          "prompt": "Explore `scripts/launch_cloud_parallel.sh`, `scripts/monitor_parallel.sh`, and `scripts/deploy_to_instances.py` for distributed scale testing."
        }
      ]
    }
  ]
}
```
