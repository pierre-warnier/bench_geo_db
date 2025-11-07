#!/usr/bin/env python3
"""SedonaDB Benchmark - Memory Optimized Version"""

import time
import sedona.db
import gc

print("=" * 80)
print("SEDONADB BENCHMARKS")
print("=" * 80)

# Initialize SedonaDB
print("\nInitializing SedonaDB...")
sd = sedona.db.connect()
print("✓ SedonaDB initialized\n")

results = {}

# Load data directly from GeoJSON files using SedonaDB's native capabilities
print("Loading data files (this may take a while)...")

# Query 1: Spatial Join - Buildings to Neighborhoods
print("\n[1/4] Query 1: Spatial Join + Aggregation")
print("  Loading data and executing query...")
query1 = """
WITH buildings AS (
    SELECT * FROM st_read('data/Building_Footprints.geojson')
),
neighborhoods AS (
    SELECT * FROM st_read('data/nyc_hoods.geojson')
)
SELECT
    COUNT(b.bin) as building_count,
    n.neighborhood
FROM neighborhoods n
JOIN buildings b
    ON ST_Intersects(b.geometry, n.geometry)
GROUP BY n.neighborhood
"""

try:
    start = time.time()
    result = sd.sql(query1)
    result.execute()
    elapsed = time.time() - start
    results['q1'] = elapsed
    print(f"  Time: {elapsed:.3f}s")
    del result
    gc.collect()
except Exception as e:
    print(f"  Error: {str(e)[:200]}")
    results['q1'] = 'ERROR'

# Query 2: Distance Within
print("\n[2/4] Query 2: Distance Within (200m)")
print("  Loading data and executing query...")
query2 = """
WITH buildings AS (
    SELECT * FROM st_read('data/Building_Footprints.geojson')
),
hydrants AS (
    SELECT * FROM st_read('data/NYCDEPCitywideHydrants.geojson')
)
SELECT
    COUNT(b.bin) as building_count,
    h.unitid
FROM hydrants h
JOIN buildings b
    ON ST_DWithin(
        ST_Transform(b.geometry, '3857'),
        ST_Transform(h.geometry, '3857'),
        200)
GROUP BY h.unitid
"""

try:
    start = time.time()
    result = sd.sql(query2)
    result.execute()
    elapsed = time.time() - start
    results['q2'] = elapsed
    print(f"  Time: {elapsed:.3f}s")
    del result
    gc.collect()
except Exception as e:
    print(f"  Error: {str(e)[:200]}")
    results['q2'] = 'ERROR'

# Query 3: Area Weighted Interpolation
print("\n[3/4] Query 3: Area Weighted Interpolation")
print("  Loading data and executing query...")
query3 = """
WITH buildings AS (
    SELECT * FROM st_read('data/Building_Footprints.geojson')
),
census AS (
    SELECT * FROM st_read('data/nys_census_blocks.geojson')
)
SELECT
    b.bin,
    SUM(
        c.population * (
            ST_Area(
                ST_Intersection(
                    ST_Buffer(ST_Transform(b.geometry, '3857'), 800),
                    ST_Transform(c.geometry, '3857')
                )
            ) / ST_Area(ST_Transform(c.geometry, '3857'))
        )
    ) as pop
FROM census c
JOIN buildings b
    ON ST_Intersects(c.geometry, b.geometry)
GROUP BY b.bin
"""

try:
    start = time.time()
    result = sd.sql(query3)
    result.execute()
    elapsed = time.time() - start
    results['q3'] = elapsed
    print(f"  Time: {elapsed:.3f}s")
    del result
    gc.collect()
except Exception as e:
    print(f"  Error: {str(e)[:200]}")
    results['q3'] = 'ERROR'

# Query 4: KNN Join
print("\n[4/4] Query 4: K-Nearest Neighbors (5)")
print("  Loading data and executing query...")
query4 = """
WITH buildings AS (
    SELECT * FROM st_read('data/Building_Footprints.geojson')
),
hydrants AS (
    SELECT * FROM st_read('data/NYCDEPCitywideHydrants.geojson')
)
SELECT
    b.bin as building_id,
    h.unitid as hydrant_id,
    ST_Distance(b.geometry, h.geometry) as distance
FROM hydrants h
JOIN buildings b
    ON ST_KNN(b.geometry, h.geometry, 5, true)
"""

try:
    start = time.time()
    result = sd.sql(query4)
    result.execute()
    elapsed = time.time() - start
    results['q4'] = elapsed
    print(f"  Time: {elapsed:.3f}s")
    del result
    gc.collect()
except Exception as e:
    print(f"  Error: {str(e)[:200]}")
    results['q4'] = 'ERROR'

print("\n✓ SedonaDB benchmarks completed")
print("\nRESULTS:")
print(f"  Query 1 (Spatial Join):             {results.get('q1', 'N/A')}")
print(f"  Query 2 (Distance Within):          {results.get('q2', 'N/A')}")
print(f"  Query 3 (Area Weighted Interp):    {results.get('q3', 'N/A')}")
print(f"  Query 4 (KNN):                      {results.get('q4', 'N/A')}")
