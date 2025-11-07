#!/usr/bin/env python3
"""DuckDB Spatial Benchmark"""

import time
import duckdb

print("=" * 80)
print("DUCKDB BENCHMARKS")
print("=" * 80)

# Initialize DuckDB
print("\nInitializing DuckDB with spatial extension...")
con = duckdb.connect()
con.execute('INSTALL spatial;')
con.execute('LOAD spatial;')
con.execute('SET enable_external_file_cache = false;')
print("✓ DuckDB initialized\n")

results = {}

# Query 1: Spatial Join
print("[1/4] Query 1: Spatial Join + Aggregation")
query1 = """
SELECT
    COUNT(a.bin) as building_count,
    b.neighborhood
FROM st_read('data/Building_Footprints.geojson') a
JOIN st_read('data/nyc_hoods.geojson') b
    ON st_intersects(a.geom, b.geom)
GROUP BY b.neighborhood
"""

start = time.time()
result = con.execute(query1)
_ = result.fetchall()
elapsed = time.time() - start
results['q1'] = elapsed
print(f"  Time: {elapsed:.3f}s")

# Query 2: Distance Within
print("[2/4] Query 2: Distance Within (200m)")
query2 = """
SELECT
    COUNT(a.bin) as building_count,
    b.unitid
FROM st_read('data/NYCDEPCitywideHydrants.geojson') b
JOIN st_read('data/Building_Footprints.geojson') a
    ON st_dwithin(
        st_transform(a.geom, 'EPSG:4326', 'EPSG:3857'),
        st_transform(b.geom, 'EPSG:4326', 'EPSG:3857'),
        200)
GROUP BY b.unitid
"""

start = time.time()
result = con.execute(query2)
_ = result.fetchall()
elapsed = time.time() - start
results['q2'] = elapsed
print(f"  Time: {elapsed:.3f}s")

# Query 3: Area Weighted Interpolation
print("[3/4] Query 3: Area Weighted Interpolation")
query3 = """
SELECT
    b.bin,
    SUM(
        a.population * (
            st_area(
                st_intersection(st_buffer(st_transform(b.geom, 'EPSG:4326', 'EPSG:3857'), 800),
                               st_transform(a.geom, 'EPSG:4326', 'EPSG:3857'))
            ) / st_area(st_transform(a.geom, 'EPSG:4326', 'EPSG:3857'))
        )
    ) as pop
FROM st_read('data/nys_census_blocks.geojson') a
JOIN st_read('data/Building_Footprints.geojson') b
    ON st_intersects(a.geom, b.geom)
GROUP BY b.bin
"""

start = time.time()
result = con.execute(query3)
_ = result.fetchall()
elapsed = time.time() - start
results['q3'] = elapsed
print(f"  Time: {elapsed:.3f}s")

# Query 4: KNN Join
print("[4/4] Query 4: K-Nearest Neighbors (5)")
query4 = """
SELECT
    a.bin as building_id,
    b.unitid as hydrant_id,
    b.distance
FROM st_read('data/Building_Footprints.geojson') a
CROSS JOIN LATERAL (
    SELECT
        unitid,
        st_distance(a.geom, geom) as distance
    FROM st_read('data/NYCDEPCitywideHydrants.geojson')
    ORDER BY distance
    LIMIT 5
) b
LIMIT 1000
"""

try:
    start = time.time()
    result = con.execute(query4)
    _ = result.fetchall()
    elapsed = time.time() - start
    results['q4'] = elapsed
    print(f"  Time: {elapsed:.3f}s")
except Exception as e:
    print(f"  Error: {str(e)[:100]}")
    results['q4'] = 'ERROR'

print("\n✓ DuckDB benchmarks completed")
print("\nRESULTS:")
print(f"  Query 1 (Spatial Join):             {results['q1']:.3f}s")
print(f"  Query 2 (Distance Within):          {results['q2']:.3f}s")
print(f"  Query 3 (Area Weighted Interp):    {results['q3']:.3f}s")
print(f"  Query 4 (KNN):                      {results['q4']}")
