#!/usr/bin/env python3
"""
PostGIS Spatial Benchmark - Parquet Format

This benchmark runs 4 spatial queries on PostGIS with Parquet-loaded data:
1. Spatial Join + Aggregation
2. Distance Within (200m buffer)
3. Area Weighted Interpolation
4. K-Nearest Neighbors (using <-> KNN operator)

Note: PostGIS tables must have pre-transformed geometry columns (geom_3857, geom_4326)
with GIST indexes. Use load_parquet_to_postgis.py to set up data.
"""

import time
import psycopg2


def connect_postgis():
    """Connect to PostGIS database"""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password="postgres"
    )


def run_query(cur, name, query, results, results_key):
    """Execute a spatial query and record timing"""
    print(f"\n[Query] {name}")
    try:
        start = time.time()
        cur.execute(query)
        _ = cur.fetchall()
        elapsed = time.time() - start
        results[results_key] = elapsed
        print(f"  Time: {elapsed:.3f}s")
    except Exception as e:
        print(f"  Error: {str(e)[:200]}")
        results[results_key] = 'ERROR'


def main():
    """Run all PostGIS spatial benchmarks"""
    print("=" * 80)
    print("POSTGIS BENCHMARKS (Parquet)")
    print("=" * 80)

    conn = connect_postgis()
    cur = conn.cursor()
    results = {}

    print("\n=" * 80)
    print("Running Benchmark Queries")
    print("=" * 80)

    # Query 1: Spatial Join + Aggregation
    # Count buildings per neighborhood using ST_Intersects
    # Uses geom_4326 (native CRS) with GIST index
    query1 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.neighborhood
    FROM buildings_parquet a
    JOIN neighborhoods_parquet b
        ON ST_Intersects(a.geom_4326, b.geom_4326)
    GROUP BY b.neighborhood
    """
    run_query(cur, "Spatial Join + Aggregation", query1, results, 'q1')

    # Query 2: Distance Within (200m)
    # Count buildings within 200m of each hydrant
    # Uses pre-transformed geom_3857 columns (GIST indexed)
    # Advantage: No runtime transformation (unlike DuckDB)
    query2 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.unitid
    FROM buildings_parquet a
    JOIN hydrants_parquet b
        ON ST_DWithin(a.geom_3857, b.geom_3857, 200)
    GROUP BY b.unitid
    """
    run_query(cur, "Distance Within (200m)", query2, results, 'q2')

    # Query 3: Area Weighted Interpolation
    # Estimate population per building using census block overlap
    # Runtime transformations for buffer and area calculations
    query3 = """
    SELECT
        b.bin,
        SUM(
            a.population * (
                ST_Area(
                    ST_Intersection(
                        ST_Buffer(ST_Transform(b.geom_4326, 'EPSG:4326', 'EPSG:3857'), 800),
                        ST_Transform(a.geom_4326, 'EPSG:4326', 'EPSG:3857')
                    )
                ) / ST_Area(ST_Transform(a.geom_4326, 'EPSG:4326', 'EPSG:3857'))
            )
        ) as pop
    FROM census_blocks_parquet a
    JOIN buildings_parquet b
        ON ST_Intersects(a.geom_4326, b.geom_4326)
    GROUP BY b.bin
    """
    run_query(cur, "Area Weighted Interpolation", query3, results, 'q3')

    # Query 4: K-Nearest Neighbors
    # Find 5 nearest hydrants for each building using <-> KNN operator
    # GIST index on geom_3857 enables index-aware KNN search
    # CROSS JOIN LATERAL processes row-by-row (not parallelized)
    query4 = """
    SELECT
        a.bin as building_id,
        b.unitid as hydrant_id,
        b.distance
    FROM buildings_parquet a
    CROSS JOIN LATERAL (
        SELECT
            unitid,
            ST_Distance(a.geom_3857, geom_3857) as distance
        FROM hydrants_parquet
        ORDER BY a.geom_3857 <-> geom_3857
        LIMIT 5
    ) b
    """
    run_query(cur, "K-Nearest Neighbors (5)", query4, results, 'q4')

    # Print results
    print("\n" + "=" * 80)
    print("✓ PostGIS benchmarks completed")
    print("=" * 80)
    print("\nRESULTS:")
    for i, (name, key) in enumerate([
        ('Spatial Join', 'q1'),
        ('Distance Within', 'q2'),
        ('Area Weighted Interp', 'q3'),
        ('KNN (full dataset)', 'q4')
    ], 1):
        val = results.get(key, 'N/A')
        if isinstance(val, float):
            print(f"  Query {i} ({name:23s}): {val:.3f}s")
        else:
            print(f"  Query {i} ({name:23s}): {val}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
