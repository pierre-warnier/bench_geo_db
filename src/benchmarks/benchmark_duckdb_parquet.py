#!/usr/bin/env python3
"""
DuckDB Spatial Benchmark - Parquet Format

This benchmark loads Parquet files into DuckDB tables and runs 4 spatial queries:
1. Spatial Join + Aggregation
2. Distance Within (200m buffer)
3. Area Weighted Interpolation
4. K-Nearest Neighbors (limited to 1000 buildings - full dataset causes OOM)

Note: DuckDB requires pre-loading Parquet into tables via CREATE TABLE AS SELECT.
Direct read_parquet() hangs on GROUP BY spatial operations.
"""

import time
import duckdb


def initialize_duckdb():
    """Initialize DuckDB connection with spatial extension"""
    print("\n Initializing DuckDB with spatial extension...")
    con = duckdb.connect()
    con.execute('INSTALL spatial;')
    con.execute('LOAD spatial;')
    print("✓ DuckDB initialized\n")
    return con


def load_parquet_tables(con):
    """
    Load Parquet files into DuckDB tables.

    IMPORTANT: Must use CREATE TABLE AS SELECT * FROM read_parquet()
    Direct querying of read_parquet() hangs on GROUP BY spatial operations.
    """
    print("Loading Parquet files into DuckDB tables...")

    tables = [
        ('buildings', 'data/buildings.parquet'),
        ('hydrants', 'data/hydrants.parquet'),
        ('neighborhoods', 'data/neighborhoods.parquet'),
        ('census_blocks', 'data/census_blocks.parquet')
    ]

    for i, (table_name, file_path) in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Loading {table_name}...")
        start = time.time()
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{file_path}')
        """)
        elapsed = time.time() - start
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ Loaded {row_count:,} rows ({elapsed:.3f}s)")

    print("\n✓ All data loaded into DuckDB\n")


def run_query(con, name, query, results_key, results, note=""):
    """Execute a spatial query and record timing"""
    print(f"\n[Query] {name}")
    try:
        start = time.time()
        result = con.execute(query)
        _ = result.fetchall()
        elapsed = time.time() - start
        results[results_key] = elapsed
        print(f"  Time: {elapsed:.3f}s {note}")
    except Exception as e:
        print(f"  Error: {str(e)[:200]}")
        results[results_key] = 'ERROR'


def main():
    """Run all DuckDB spatial benchmarks"""
    print("=" * 80)
    print("DUCKDB BENCHMARKS (Parquet - Pre-loaded via SQL)")
    print("=" * 80)

    # Initialize
    con = initialize_duckdb()
    results = {}

    # Load data
    load_parquet_tables(con)

    print("=" * 80)
    print("Running Benchmark Queries")
    print("=" * 80)

    # Query 1: Spatial Join + Aggregation
    # Count buildings per neighborhood using ST_Intersects
    query1 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.neighborhood
    FROM buildings a
    JOIN neighborhoods b
        ON st_intersects(a.geometry, b.geometry)
    GROUP BY b.neighborhood
    """
    run_query(con, "Spatial Join + Aggregation", query1, 'q1', results)

    # Query 2: Distance Within (200m)
    # Count buildings within 200m of each hydrant
    # Requires transform to EPSG:3857 (Web Mercator) for metric distances
    # FIX: Use always_xy := true to correct axis order (GitHub issue #474)
    query2 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.unitid
    FROM buildings a
    JOIN hydrants b
        ON st_dwithin(
            st_transform(a.geometry, 'EPSG:4326', 'EPSG:3857', always_xy := true),
            st_transform(b.geometry, 'EPSG:4326', 'EPSG:3857', always_xy := true),
            200)
    GROUP BY b.unitid
    """
    run_query(con, "Distance Within (200m)", query2, 'q2', results)

    # Query 3: Area Weighted Interpolation
    # Estimate population per building using census block overlap
    query3 = """
    SELECT
        b.bin,
        SUM(
            a.population * (
                st_area(
                    st_intersection(
                        st_buffer(st_transform(b.geometry, 'EPSG:4326', 'EPSG:3857'), 800),
                        st_transform(a.geometry, 'EPSG:4326', 'EPSG:3857')
                    )
                ) / st_area(st_transform(a.geometry, 'EPSG:4326', 'EPSG:3857'))
            )
        ) as pop
    FROM census_blocks a
    JOIN buildings b
        ON st_intersects(a.geometry, b.geometry)
    GROUP BY b.bin
    """
    run_query(con, "Area Weighted Interpolation", query3, 'q3', results)

    # Query 4: K-Nearest Neighbors
    # Find 5 nearest hydrants for each building
    # LIMITED to 1000 buildings - full dataset (1.2M) causes OOM
    # DuckDB lacks <-> KNN operator, must compute ALL distances
    query4 = """
    WITH sample_buildings AS (
        SELECT * FROM buildings
        LIMIT 1000
    )
    SELECT
        a.bin as building_id,
        b.unitid as hydrant_id,
        b.distance
    FROM sample_buildings a
    CROSS JOIN LATERAL (
        SELECT
            unitid,
            st_distance(a.geometry, geometry) as distance
        FROM hydrants
        ORDER BY distance
        LIMIT 5
    ) b
    """
    run_query(con, "K-Nearest Neighbors (5)", query4, 'q4', results,
             note="(limited to 1000 buildings)")

    # Print results
    print("\n" + "=" * 80)
    print("✓ DuckDB benchmarks completed")
    print("=" * 80)
    print("\nRESULTS:")
    for i, (name, key) in enumerate([
        ('Spatial Join', 'q1'),
        ('Distance Within', 'q2'),
        ('Area Weighted Interp', 'q3'),
        ('KNN (sample 1000)', 'q4')
    ], 1):
        val = results.get(key, 'N/A')
        if isinstance(val, float):
            print(f"  Query {i} ({name:23s}): {val:.3f}s")
        else:
            print(f"  Query {i} ({name:23s}): {val}")


if __name__ == '__main__':
    main()
