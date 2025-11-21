# Geospatial Database Benchmark: HeavyDB vs SedonaDB vs DuckDB vs PostGIS

Complete replication and extension of the benchmark from https://forrest.nyc/sedonadb-vs-duckdb-vs-postgis-which-spatial-sql-engine-is-fastest/

**Extended with GPU-accelerated HeavyDB benchmarks**

## Quick Results Summary

### Performance Winner: **HeavyDB (GPU)**
- **0.166s - 0.537s** for all queries
- **40-930x faster** than PostGIS
- Requires NVIDIA GPU + grid-based equijoin technique

### Best Without GPU: **SedonaDB** (with Parquet)
- **0.220s - 6.7s** for all queries
- **15-36x faster** than PostGIS
- Excellent native ST_KNN support

### Analytical Workloads: **DuckDB** (with Parquet)
- **0.9s - 9.7s** for Q1-Q3
- **6-25x faster** than PostGIS on supported queries
- KNN requires H3 index workaround (82s with H3)

### Production GIS: **PostGIS**
- **6.6s - 241s** for all queries
- Most complete spatial SQL implementation
- Best choice for complex operations

---

## Dataset

NYC Open Data: **1.2M building footprints** (polygons), **109K fire hydrants** (points), **312 neighborhoods** (polygons), and **16K census blocks** (polygons). Total ~1.4M geometries representing real-world urban spatial data.

## Queries

- **Q1 - Spatial Join**: Count how many buildings fall within each neighborhood polygon.
- **Q2 - Distance Within**: Find all building-hydrant pairs within 200 meters of each other.
- **Q3 - Area Interpolation**: Estimate building population by weighting census block overlap areas.
- **Q4 - KNN**: Find the 5 nearest hydrants to each of the 1.2M buildings.

| Query | HeavyDB (GPU) | SedonaDB | DuckDB (Parquet) | PostGIS |
|-------|---------------|----------|------------------|---------|
| **Q1: Spatial Join** | **0.166s** | 0.220s | 1.105s | 6.639s |
| **Q2: Distance Within** | **0.438s** | 1.353s | 0.915s | 20.362s |
| **Q3: Area Weighted** | **0.259s** | 6.668s | 9.729s | 241.587s |
| **Q4: KNN (5 nearest)** | **0.537s** | 1.724s | 82.415s (H3) | 57.401s |

---

## Key Discoveries

### 1. HeavyDB GPU Acceleration Requires Grid-Based Equijoin

HeavyDB crashes on pure spatial joins without an equality condition. The solution:

```sql
-- Add grid columns for hash join
ALTER TABLE buildings ADD COLUMN grid_x INT;
ALTER TABLE buildings ADD COLUMN grid_y INT;
UPDATE buildings SET
    grid_x = CAST(FLOOR(ST_X(geom) * 100) AS INT),
    grid_y = CAST(FLOOR(ST_Y(geom) * 100) AS INT);

-- Query pattern (required for GPU acceleration)
SELECT ... FROM buildings b
JOIN hydrants h ON b.grid_x = h.grid_x AND b.grid_y = h.grid_y
               AND ST_DWITHIN(h.geom, b.geom, 0.0017)
```

### 2. DuckDB ST_Transform Axis Order Bug

DuckDB spatial extension has an axis order issue (GitHub #474). Without the fix, coordinates are swapped:

```sql
-- WRONG (axes swapped)
ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857')

-- CORRECT (use always_xy parameter)
ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857', always_xy := true)
```

### 3. Fair Comparison Validation

All databases now produce equivalent results for Q2 (200m distance):

| Database | Result Count | Notes |
|----------|--------------|-------|
| PostGIS | 17,061,933 pairs | Reference (EPSG:3857) |
| SedonaDB | 17,061,933 pairs | Matches PostGIS |
| DuckDB | 17,061,933 pairs | With `always_xy := true` fix |
| HeavyDB | 17,153,441 pairs | +0.5% (degree-based threshold) |

### 4. DuckDB KNN Solution: H3 Spatial Index

DuckDB lacks native KNN operator but H3 hexagonal index provides a workaround:
- **H3 k-ring filtering** reduces comparisons from 135B to 441M (307x reduction)
- **82s performance** on full dataset (vs PostGIS 57s)
- **99.986% complete** results

---

## Recommendations

| Use Case | Recommended Database |
|----------|---------------------|
| Maximum performance (GPU available) | HeavyDB with grid equijoin |
| Large-scale analytics (no GPU) | SedonaDB |
| Data lake / ETL workflows | DuckDB (Parquet native) |
| Production GIS application | PostGIS |
| Complex spatial SQL | PostGIS or SedonaDB |
| KNN on full dataset (no GPU) | SedonaDB |

---

## Project Structure

```
bench_geo_db/
├── src/
│   ├── benchmarks/
│   │   ├── benchmark_postgis.py
│   │   ├── benchmark_postgis_parquet.py
│   │   ├── benchmark_duckdb.py
│   │   ├── benchmark_duckdb_parquet.py
│   │   ├── benchmark_duckdb_knn_h3.py
│   │   ├── benchmark_sedona.py
│   │   ├── benchmark_sedona_parquet.py
│   │   ├── benchmark_heavydb_working.py      # HeavyDB with grid equijoin
│   │   └── benchmark_fair_comparison.py      # Cross-database validation
│   └── utils/
│       ├── convert_to_parquet.py
│       ├── load_parquet_to_postgis.py
│       └── load_parquet_to_heavydb.py
├── data/                    # Data files (not in repo)
│   ├── *.geojson           # Original NYC Open Data
│   └── *.parquet           # Optimized format
├── docker-compose.yml
├── BENCHMARK_RESULTS.txt    # Full benchmark results
└── README.md
```

---

## Setup Instructions

### 1. Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate

pip install geopandas duckdb psycopg2-binary sqlalchemy apache-sedona[db] heavyai
```

### 2. Convert Data to Parquet

```bash
python src/utils/convert_to_parquet.py
```

### 3. Database Setup

**PostGIS**
```bash
docker run --name bench_postgis -d \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgis/postgis:17-3.5

python src/utils/load_parquet_to_postgis.py
```

**HeavyDB (GPU)**
```bash
docker run --name heavydb --gpus all -d \
  -p 6274:6274 -p 6278:6278 \
  -v heavyai-storage:/var/lib/heavyai \
  heavyai/heavydb-ee:latest

python src/utils/load_parquet_to_heavydb.py
```

### 4. Run Benchmarks

```bash
# PostGIS
python src/benchmarks/benchmark_postgis_parquet.py

# DuckDB
python src/benchmarks/benchmark_duckdb_parquet.py

# DuckDB KNN with H3
python src/benchmarks/benchmark_duckdb_knn_h3.py

# SedonaDB
python src/benchmarks/benchmark_sedona_parquet.py

# HeavyDB (GPU)
python src/benchmarks/benchmark_heavydb_working.py

# Fair comparison validation
python src/benchmarks/benchmark_fair_comparison.py
```

---

## Hardware

- **Machine**: SILAP-76
- **GPU**: NVIDIA RTX 2000 Ada Generation Laptop GPU (8GB VRAM)
- **RAM**: 20GB limit for tests
- **Platform**: Linux 6.14.0-35-generic

---

## Technology Notes

### PostGIS
- Most mature and complete spatial SQL implementation
- GIST indexes provide reasonable performance
- Best choice for complex spatial operations and production use

### DuckDB
- Excellent analytical performance with columnar storage
- Parquet-native for data lake integration
- **Bug**: ST_Transform requires `always_xy := true` (GitHub #474)
- KNN requires H3 spatial indexing workaround

### SedonaDB (Apache Sedona)
- Spark-based distributed spatial analytics
- Excellent native ST_KNN support
- No GPU requirement
- Ideal for big data spatial processing

### HeavyDB (GPU-Accelerated)
- **Fastest overall** when properly configured
- **Requires**: Grid-based equijoin + ST_DWITHIN pattern
- **Without equijoin**: HeavyDB crashes on spatial joins
- Grid cells at 0.01° (~1km) for hash join optimization

---

## Bottom Line

**Four viable options depending on your needs:**

1. **HeavyDB (GPU)**: Absolute fastest (0.17s - 0.54s), requires GPU + grid technique
2. **SedonaDB**: Best without GPU (0.22s - 6.7s), excellent KNN support
3. **DuckDB**: Fast analytics (0.9s - 9.7s), Parquet-native, H3 for KNN
4. **PostGIS**: Most reliable (6.6s - 241s), production-ready, works with anything

**Key insight**: HeavyDB GPU acceleration delivers 40-930x speedup over PostGIS, but requires the grid-based equijoin technique. Without it, queries crash.

---

**Status**: Complete - All benchmarks run, fair comparison validated, DuckDB axis bug fixed
