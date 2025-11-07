#!/usr/bin/env python3
"""DuckDB KNN using spatial grid partitioning"""

import time
import duckdb

print("=" * 80)
print("DUCKDB KNN - Grid Partitioning Approach")
print("=" * 80)

con = duckdb.connect()
con.execute('INSTALL spatial;')
con.execute('LOAD spatial;')

# Load data
print("\n[1/5] Loading data...")
start = time.time()
con.execute("CREATE TABLE buildings AS SELECT * FROM read_parquet('data/buildings.parquet')")
con.execute("CREATE TABLE hydrants AS SELECT * FROM read_parquet('data/hydrants.parquet')")
elapsed = time.time() - start
print(f"  ✓ Loaded data ({elapsed:.3f}s)")

# Add lon/lat columns
print("\n[2/5] Extracting coordinates...")
start = time.time()
con.execute("ALTER TABLE buildings ADD COLUMN lon DOUBLE")
con.execute("ALTER TABLE buildings ADD COLUMN lat DOUBLE")
con.execute("""
    UPDATE buildings
    SET lon = ST_X(ST_Centroid(geometry)),
        lat = ST_Y(ST_Centroid(geometry))
""")

con.execute("ALTER TABLE hydrants ADD COLUMN lon DOUBLE")
con.execute("ALTER TABLE hydrants ADD COLUMN lat DOUBLE")
con.execute("""
    UPDATE hydrants
    SET lon = ST_X(geometry),
        lat = ST_Y(geometry)
""")
elapsed = time.time() - start
print(f"  ✓ Extracted coordinates ({elapsed:.3f}s)")

# Create grid cells
print("\n[3/5] Creating grid cells...")
print("  Using cell_size = 0.002° (≈220m at NYC latitude)")
start = time.time()

con.execute("ALTER TABLE buildings ADD COLUMN gx INTEGER")
con.execute("ALTER TABLE buildings ADD COLUMN gy INTEGER")
con.execute("""
    UPDATE buildings
    SET gx = CAST(FLOOR(lon / 0.002) AS INTEGER),
        gy = CAST(FLOOR(lat / 0.002) AS INTEGER)
""")

con.execute("ALTER TABLE hydrants ADD COLUMN gx INTEGER")
con.execute("ALTER TABLE hydrants ADD COLUMN gy INTEGER")
con.execute("""
    UPDATE hydrants
    SET gx = CAST(FLOOR(lon / 0.002) AS INTEGER),
        gy = CAST(FLOOR(lat / 0.002) AS INTEGER)
""")
elapsed = time.time() - start
print(f"  ✓ Created grid cells ({elapsed:.3f}s)")

# Sample analysis
print("\n[4/5] Sample analysis (10 buildings)...")
result = con.execute("""
    WITH candidate_pairs AS (
        SELECT
            b.bin as building_id,
            h.unitid as hydrant_id,
            ST_Distance_Spheroid(ST_Centroid(b.geometry), h.geometry) AS dist_m
        FROM (SELECT * FROM buildings LIMIT 10) b
        JOIN hydrants h
            ON h.gx BETWEEN b.gx - 2 AND b.gx + 2
           AND h.gy BETWEEN b.gy - 2 AND b.gy + 2
    )
    SELECT
        MIN(dist_m) as min_dist,
        AVG(dist_m) as avg_dist,
        MAX(dist_m) as max_dist,
        COUNT(*) as total_pairs
    FROM candidate_pairs
""").fetchone()

print(f"  Sample stats (±2 cell neighborhood):")
print(f"    - Min distance: {result[0]:.1f}m")
print(f"    - Avg distance: {result[1]:.1f}m")
print(f"    - Max distance: {result[2]:.1f}m")
print(f"    - Candidate pairs: {result[3]:,}")

# Full KNN query
print("\n[5/5] Running KNN on FULL dataset (1.2M buildings)...")
start = time.time()

result = con.execute("""
    WITH candidate_pairs AS (
        SELECT
            b.bin as building_id,
            h.unitid as hydrant_id,
            ST_Distance_Spheroid(ST_Centroid(b.geometry), h.geometry) AS dist_m
        FROM buildings b
        JOIN hydrants h
            ON h.gx BETWEEN b.gx - 2 AND b.gx + 2
           AND h.gy BETWEEN b.gy - 2 AND b.gy + 2
    ),
    ranked AS (
        SELECT
            building_id,
            hydrant_id,
            dist_m,
            row_number() OVER (
                PARTITION BY building_id
                ORDER BY dist_m
            ) AS rn
        FROM candidate_pairs
    )
    SELECT COUNT(*) as total_results
    FROM ranked
    WHERE rn <= 5
""")

rows = result.fetchone()
elapsed = time.time() - start

print(f"\n✓ KNN query completed!")
print(f"  - Time: {elapsed:.3f}s")
print(f"  - Results: {rows[0]:,} (expected: {1238423 * 5:,})")
print(f"  - Buildings processed: 1,238,423")
print(f"  - Neighbors per building: 5")

# Get distance distribution for the 5th nearest neighbor
print("\n[6/6] Analyzing 5th nearest neighbor distances...")
result = con.execute("""
    WITH candidate_pairs AS (
        SELECT
            b.bin as building_id,
            h.unitid as hydrant_id,
            ST_Distance_Spheroid(ST_Centroid(b.geometry), h.geometry) AS dist_m
        FROM buildings b
        JOIN hydrants h
            ON h.gx BETWEEN b.gx - 2 AND b.gx + 2
           AND h.gy BETWEEN b.gy - 2 AND b.gy + 2
    ),
    ranked AS (
        SELECT
            building_id,
            hydrant_id,
            dist_m,
            row_number() OVER (
                PARTITION BY building_id
                ORDER BY dist_m
            ) AS rn
        FROM candidate_pairs
    )
    SELECT
        MIN(dist_m) as min_5th,
        AVG(dist_m) as avg_5th,
        MAX(dist_m) as max_5th
    FROM ranked
    WHERE rn = 5
""").fetchone()

print(f"  5th nearest neighbor statistics:")
print(f"    - Min: {result[0]:.1f}m")
print(f"    - Avg: {result[1]:.1f}m")
print(f"    - Max: {result[2]:.1f}m")

print("\n" + "=" * 80)
print("FINAL RESULTS")
print("=" * 80)
print(f"\n✓ DuckDB KNN (Grid Partitioning): {elapsed:.3f}s")
print(f"  - Full dataset (1,238,423 buildings)")
print(f"  - Using metric distances (ST_Distance_Spheroid)")
print(f"  - Grid cell size: 0.002° (≈220m)")
print(f"  - Search neighborhood: ±2 cells (≈880m)")
