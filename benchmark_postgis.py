#!/usr/bin/env python3
"""PostGIS Benchmark"""

import time
import psycopg2

print("=" * 80)
print("POSTGIS BENCHMARKS")
print("=" * 80)

# Connect to PostGIS
print("\nConnecting to PostGIS...")
conn = psycopg2.connect(
    host="localhost",
    database="gis",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()
print("✓ Connected to PostGIS\n")

results = {}

# Query 1: Spatial Join
print("[1/4] Query 1: Spatial Join + Aggregation")
query1 = """
SELECT
    COUNT(a.bin) as building_count,
    b.neighborhood
FROM neighborhoods b
JOIN buildings a
    ON ST_Intersects(a.geom_4326, b.geom_4326)
GROUP BY b.neighborhood
"""

start = time.time()
cur.execute(query1)
_ = cur.fetchall()
elapsed = time.time() - start
results['q1'] = elapsed
print(f"  Time: {elapsed:.3f}s")

# Query 2: Distance Within
print("[2/4] Query 2: Distance Within (200m)")
query2 = """
SELECT
    COUNT(a.bin) as building_count,
    b.unitid
FROM hydrants b
JOIN buildings a
    ON ST_DWithin(a.geom_3857, b.geom_3857, 200)
GROUP BY b.unitid
"""

start = time.time()
cur.execute(query2)
_ = cur.fetchall()
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
            ST_Area(
                ST_Intersection(ST_Buffer(ST_Transform(b.wkb_geometry, 3857), 800),
                               ST_Transform(a.wkb_geometry, 3857))
            ) / ST_Area(ST_Transform(a.wkb_geometry, 3857))
        )
    ) as pop
FROM census_blocks a
JOIN buildings b
    ON ST_Intersects(a.geom_4326, b.geom_4326)
GROUP BY b.bin
"""

start = time.time()
cur.execute(query3)
_ = cur.fetchall()
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
FROM buildings a
CROSS JOIN LATERAL (
    SELECT
        unitid,
        ST_Distance(a.geom_3857, geom_3857) as distance
    FROM hydrants
    ORDER BY a.geom_3857 <-> geom_3857
    LIMIT 5
) b
"""

start = time.time()
cur.execute(query4)
_ = cur.fetchall()
elapsed = time.time() - start
results['q4'] = elapsed
print(f"  Time: {elapsed:.3f}s")

cur.close()
conn.close()

print("\n✓ PostGIS benchmarks completed")
print("\nRESULTS:")
print(f"  Query 1 (Spatial Join):             {results['q1']:.3f}s")
print(f"  Query 2 (Distance Within):          {results['q2']:.3f}s")
print(f"  Query 3 (Area Weighted Interp):    {results['q3']:.3f}s")
print(f"  Query 4 (KNN):                      {results['q4']:.3f}s")
