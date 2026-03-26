# Reflection.md Analysis: Technical Insights

Source: Discussion between user and AI assistant "Aurelle" about spatial optimization for the ARC Capstone pipeline.

---

## 1. Core Problem Identified

**Root cause of "all-zero" FAST output**: Spatial logic mismatch — not a data quality bug.

- Pipeline processed 31M rows → 1.4M after firmzone filter → only 3,801 rows actually inside the raster
- **0.268% spatial selectivity** = 99.7% of FAST computation was wasted on dry-land properties
- The `impact-only` filter used FEMA FIRM flood zones (long-term risk classification) instead of the actual hurricane event footprint (Beryl raster)
- Analogy: filtering "everyone who owns an umbrella" instead of "everyone who was rained on"

**Key distinction**: Hazard Footprint (event raster) must be the FIRST filter gate, not firmzone.

---

## 2. Spatial Optimization Recommendations

### Priority 1: Raster Mask Pre-filter (Immediate Fix)
- Force all input records through `ST_Within(point, raster_bbox)` before FAST
- Better: use **Raster Valid Mask** (non-zero pixels), not just BBox — flood zones are irregular, BBox includes dry land
- Extract via `rasterio.bounds` + `valid_mask` → convert to Convex Hull or Alpha Shape
- Expected result: task size drops from 1.4M → ~4,000 rows; runtime from 603s → seconds

### Priority 2: H3 Hexagonal Indexing (Scalable Pre-filter)
- Pre-compute H3 cell IDs for all 30M property points (64-bit integer, one-time cost)
- Convert raster valid footprint → set of H3 cell IDs
- Filter: `point.h3_id in raster_h3_set` — integer hash match, no geometry computation
- Eliminates 99.7% of records before any spatial join
- Library: `h3-py` (Uber open source)

### Priority 3: DuckDB Spatial Extension (Performance)
- Best single-machine tool for 30M-row CSV/Parquet spatial filtering
- SQL: `SELECT * FROM properties WHERE ST_Within(ST_Point(lon, lat), raster_envelope)`
- 10-100x faster than pandas/geopandas for this scale
- Supports direct Parquet queries without full memory load

### Priority 4: STAC Catalog (Multi-event Management)
- For managing multiple hurricane events / raster versions
- Generate STAC Item JSON per raster with precise polygon boundary + timestamp
- Query STAC index before FAST run → auto-identify which states/counties are covered
- Libraries: `pystac`, `pystac-client`
- Reference: Microsoft Planetary Computer, USGS/FEMA STAC catalogs

### Priority 5: Apache Sedona / Dask-GeoPandas (Distributed Scale)
- Apache Sedona (formerly GeoSpark): Spark-based distributed raster-vector joins
- Dask-GeoPandas: parallel GeoPandas with spatial partitioning (Q-Tree / Morton curve)
- Relevant only if single-machine DuckDB proves insufficient

---

## 3. Research Papers Referenced

| Paper | arXiv ID | Relevance |
|-------|----------|-----------|
| Apache Sedona: High-performance Spatio-temporal Processing | 2407.15174 | Distributed raster-vector join optimization |
| Geospatial Big Data Handling with HPC | 1907.12182 | Spatial partitioning to reduce I/O redundancy |
| Smart Flood Resilience: Big Data for Rapid Assessment | 2111.06461 | Community-scale flood impact in first hours post-disaster |
| FloodGenome: Interpretable ML for Property Flood Risk | 2403.10625 | Property-level flood risk feature analysis |
| High-resolution Global Flood Risk Analysis | 2103.04131 | Scalable loss analysis under high-res rasters |
| Integrated GIS framework: Hurricane Michael | 2412.13728 | GIS → shelter/infrastructure accessibility planning |
| GeoAI for Satellite Flood Extent Mapping | 2504.02214 | AI-based flood extent detection |
| xBD: Building Damage from Satellite Imagery | 1911.09296 | Benchmark dataset for damage assessment |
| Global Flood Prediction: Multimodal ML | 2301.12548 | ML approach to flood prediction |
| H3: Uber's Hexagonal Hierarchical Spatial Index | (blog) | Core engineering reference for grid-based filtering |

Note: Paper "2210.12345" (Scalable Integration of Vector and Raster Data) appears to be a placeholder/example citation — verify before citing.

---

## 4. STAC Catalog Recommendations

- Use STAC to catalog each SLOSH/hurricane raster with: geometry footprint, event name, advisory number, timestamp
- Enables automatic state/county selection before FAST runs
- Replaces manual `Event_State_Mapping` with spatial query
- Reference implementations: `stac-fastapi`, Microsoft Planetary Computer architecture

---

## 5. BBox Filtering and Convex Hull Strategy

- BBox alone is insufficient for irregular flood footprints (hurricane paths are elongated)
- Recommended progression:
  1. BBox filter (fast, coarse) — eliminates most records
  2. Convex Hull of valid raster pixels — better approximation
  3. Alpha Shape of valid pixels — most precise, highest compute cost
- Implementation: `rasterio` → extract `valid_mask` → `shapely` convex_hull

---

## 6. FltyId Deduplication

- Problem: 70,000+ duplicate FltyId records within same state
- Cause: "one parcel, multiple structures" or attached buildings sharing coordinates (common in CoreLogic/Microsoft Buildings data)
- Recommended approaches:
  1. `groupby(FltyId, State, flC).max()` — keep worst-case loss record
  2. Generate composite key: `UUID = FltyId + Lat + Lon`
- Note: duplication is NOT the cause of zero-loss output; it causes loss double-counting

---

## 7. Event-State Mapping Strategy

- Current problem: pipeline runs all 16 states regardless of hurricane track
- Beryl raster only covers small portion of TX/LA — running 14 other states is pure waste
- Solutions:
  1. Manual `Event_State_Mapping` lookup table (simple, immediate)
  2. STAC spatial query (automated, scalable)
  3. `State_BBox_Lookup` table — use raster centroid/centroid to trigger state-specific FAST subtasks
- HURREVAC integration: use HURREVAC warning zone output to dynamically generate state list

---

## 8. Hazus FAST Output Analysis Findings

- FAST ran 32 subtasks successfully, processed 1,418,177 lines
- Output: ~99.7% zero-loss records — technically correct but business-invalid
- Root cause: firmzone filter ≠ event footprint filter
- FAST is a depth-damage function (DDF) point-raster calculator — it correctly returns 0 for points outside raster
- The pipeline's "impact-only" mode was designed to reduce scope but used wrong spatial criterion
- Fix: enforce raster spatial constraint BEFORE generating `run_manifest.json`

---

## 9. ML/AI Approaches Discussed

Aurelle surfaced these from paper search (not directly recommended for implementation):

- **Satellite imagery + deep learning** for flood extent mapping (GeoAI, EvaNet, Flood-LDM)
- **Graph Transformers** for flood susceptibility mapping
- **xBD dataset** for building damage classification from satellite imagery
- **DamageCAT** (Texas A&M): transformer-based typology damage categorization
- **FloodGenome**: interpretable ML for property flood risk features

**Assessment**: These are satellite/CV-based approaches. The ARC project uses physics-based SLOSH rasters + Hazus FAST, so ML is relevant only for:
- Post-processing: predicting shelter demand from FAST damage states
- Validation: cross-checking FAST outputs against satellite-observed flood extents

---

## 10. Population/Shelter Impact Gap (Critical)

Aurelle identified a **missing layer** between FAST output and ARC's actual goal:

- FAST outputs: property damage % and dollar loss
- ARC needs: displaced population count + shelter capacity requirements
- **Required bridge**: spatial join of FAST property-level results with U.S. Census Bureau Block/Tract population data
- Algorithm: if 80% of properties in a Census Block show "Major Damage" → high shelter demand weight
- This population-impact layer is NOT currently implemented in the pipeline

---

## 11. Actionable Implementation Priorities

Ranked by impact and implementation cost:

1. **[CRITICAL] Enforce raster spatial pre-filter** — add BBox/mask filter before FAST input generation
2. **[HIGH] Event-driven state selection** — use HURREVAC/raster footprint to limit states processed
3. **[HIGH] FltyId deduplication** — prevent loss double-counting in aggregated outputs
4. **[MEDIUM] DuckDB spatial queries** — replace pandas spatial ops for 30M-row filtering
5. **[MEDIUM] Population impact layer** — join FAST output with Census data for shelter estimates
6. **[LOW] H3 indexing** — pre-compute for future multi-event scalability
7. **[LOW] STAC catalog** — for managing multiple hurricane events/advisories

---

## 12. Gaps in the Discussion

What was NOT addressed:

1. **SLOSH model specifics** — how SLOSH rasters are structured, advisory numbering, coordinate systems
2. **Tsunami vs. storm surge differences** — ARC scope includes both; discussion only covered storm surge
3. **Actual FAST output schema** — no analysis of which output columns map to what business metrics
4. **Excel/non-technical user interface** — ARC requires non-technical staff usability; no UI design discussed
5. **Validation methodology** — no discussion of how to validate FAST outputs against ground truth
6. **Real-time/operational pipeline** — discussion assumed batch processing; ARC needs near-real-time capability
7. **Multi-advisory aggregation** — how to handle multiple SLOSH advisories for the same event
8. **Confidence/uncertainty quantification** — no discussion of error bounds on damage estimates
9. **Shelter capacity data integration** — ARC needs to match displaced population to available shelter locations
10. **Code implementation** — discussion remained at architecture level; no actual code was written or reviewed
