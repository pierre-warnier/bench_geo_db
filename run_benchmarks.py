#!/usr/bin/env python3
"""
Comprehensive benchmark for SedonaDB, DuckDB, and PostGIS
"""

import time
import geopandas as gpd
import duckdb
import sedona.db
import psycopg2
from tabulate import tabulate

print("=" * 80)
print("Geospatial Database Benchmark: SedonaDB vs DuckDB vs PostGIS")
print("=" * 80)
print()

# Results dictionary
results = {
    'Spatial Join + Aggregation': {},
    'Distance Within (200m)': {},
    'Area Weighted Interpolation': {},
    'K-Nearest Neighbors (5)': {}
}

# ============================================================================
# SEDONADB BENCHMARKS
# ============================================================================
print("\n" + "=" * 80)
print("SEDONADB BENCHMARKS")
print("=" * 80)

try:
    print("\n[1/4] Loading data into SedonaDB...")
    sd = sedona.db.connect()

    # Load data via GeoPandas
    buildings = gpd.read_file('data/Building_Footprints.geojson')
    hydrants = gpd.read_file('data/NYCDEPCitywideHydrants.geojson')
    neighborhoods = gpd.read_file('data/nyc_hoods.geojson')
    census = gpd.read_file('data/nys_census_blocks.geojson')

    # Create SedonaDB DataFrames
    buildings_db = sd.create_data_frame(buildings)
    hydrants_db = sd.create_data_frame(hydrants)
    neighborhoods_db = sd.create_data_frame(neighborhoods)
    census_db = sd.create_data_frame(census)

    # Create views
    buildings_db.to_view("buildings_db")
    hydrants_db.to_view("hydrants_db")
    neighborhoods_db.to_view("neighborhoods_db")
    census_db.to_view("census_db")

    print("✓ Data loaded successfully\n")

    # Query 1: Spatial Join - Buildings to Neighborhoods
    print("[2/4] Query 1: Spatial Join + Aggregation")
    query1 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.neighborhood
    FROM neighborhoods_db b
    JOIN buildings_db a
        ON ST_Intersects(a.geometry, b.geometry)
    GROUP BY b.neighborhood
    """

    start = time.time()
    result = sd.sql(query1)
    result.execute()
    sedona_q1 = time.time() - start
    results['Spatial Join + Aggregation']['SedonaDB'] = sedona_q1
    print(f"  Time: {sedona_q1:.3f}s")

    # Query 2: Distance Within - Buildings near hydrants
    print("[3/4] Query 2: Distance Within (200m)")
    query2 = """
    SELECT
        COUNT(a.bin) as building_count,
        b.unitid
    FROM hydrants_db b
    JOIN buildings_db a
        ON ST_DWithin(
            ST_Transform(a.geometry, '3857'),
            ST_Transform(b.geometry, '3857'),
            200)
    GROUP BY b.unitid
    """

    start = time.time()
    result = sd.sql(query2)
    result.execute()
    sedona_q2 = time.time() - start
    results['Distance Within (200m)']['SedonaDB'] = sedona_q2
    print(f"  Time: {sedona_q2:.3f}s")

    # Query 3: Area Weighted Interpolation
    print("[4/4] Query 3: Area Weighted Interpolation")
    query3 = """
    SELECT
        b.bin,
        SUM(
            a.population * (
                ST_Area(
                    ST_Intersection(ST_Buffer(ST_Transform(b.geometry, '3857'), 800),
                                   ST_Transform(a.geometry, '3857'))
                ) / ST_Area(ST_Transform(a.geometry, '3857'))
            )
        ) as pop
    FROM census_db a
    JOIN buildings_db b
        ON ST_Intersects(a.geometry, b.geometry)
    GROUP BY b.bin
    """

    start = time.time()
    result = sd.sql(query3)
    result.execute()
    sedona_q3 = time.time() - start
    results['Area Weighted Interpolation']['SedonaDB'] = sedona_q3
    print(f"  Time: {sedona_q3:.3f}s")

    # Query 4: KNN Join
    print("[4/4] Query 4: K-Nearest Neighbors (5)")
    query4 = """
    SELECT
        a.bin as building_id,
        b.unitid as hydrant_id,
        ST_Distance(a.geometry, b.geometry) as distance
    FROM hydrants_db b
    JOIN buildings_db a
        ON ST_KNN(a.geometry, b.geometry, 5, true)
    """

    start = time.time()
    result = sd.sql(query4)
    result.execute()
    sedona_q4 = time.time() - start
    results['K-Nearest Neighbors (5)']['SedonaDB'] = sedona_q4
    print(f"  Time: {sedona_q4:.3f}s")

    print("\n✓ SedonaDB benchmarks completed")

except Exception as e:
    print(f"\n✗ SedonaDB error: {e}")
    results['Spatial Join + Aggregation']['SedonaDB'] = 'ERROR'
    results['Distance Within (200m)']['SedonaDB'] = 'ERROR'
    results['Area Weighted Interpolation']['SedonaDB'] = 'ERROR'
    results['K-Nearest Neighbors (5)']['SedonaDB'] = 'ERROR'


# ============================================================================
# DUCKDB BENCHMARKS
# ============================================================================
print("\n" + "=" * 80)
print("DUCKDB BENCHMARKS")
print("=" * 80)

try:
    print("\n[1/4] Initializing DuckDB with spatial extension...")
    con = duckdb.connect()
    con.execute('INSTALL spatial;')
    con.execute('LOAD spatial;')
    con.execute('SET enable_external_file_cache = false;')
    print("✓ DuckDB initialized\n")

    # Query 1: Spatial Join
    print("[2/4] Query 1: Spatial Join + Aggregation")
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
    con.execute(query1)
    duckdb_q1 = time.time() - start
    results['Spatial Join + Aggregation']['DuckDB'] = duckdb_q1
    print(f"  Time: {duckdb_q1:.3f}s")

    # Query 2: Distance Within
    print("[3/4] Query 2: Distance Within (200m)")
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
    con.execute(query2)
    duckdb_q2 = time.time() - start
    results['Distance Within (200m)']['DuckDB'] = duckdb_q2
    print(f"  Time: {duckdb_q2:.3f}s")

    # Query 3: Area Weighted Interpolation
    print("[4/4] Query 3: Area Weighted Interpolation")
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
    con.execute(query3)
    duckdb_q3 = time.time() - start
    results['Area Weighted Interpolation']['DuckDB'] = duckdb_q3
    print(f"  Time: {duckdb_q3:.3f}s")

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
    """

    start = time.time()
    try:
        con.execute(query4)
        duckdb_q4 = time.time() - start
        results['K-Nearest Neighbors (5)']['DuckDB'] = duckdb_q4
        print(f"  Time: {duckdb_q4:.3f}s")
    except Exception as e:
        print(f"  Error: {str(e)[:100]}")
        results['K-Nearest Neighbors (5)']['DuckDB'] = 'TIMEOUT/ERROR'

    print("\n✓ DuckDB benchmarks completed")

except Exception as e:
    print(f"\n✗ DuckDB error: {e}")
    if 'Spatial Join + Aggregation' not in results or 'DuckDB' not in results['Spatial Join + Aggregation']:
        results['Spatial Join + Aggregation']['DuckDB'] = 'ERROR'
    if 'Distance Within (200m)' not in results or 'DuckDB' not in results['Distance Within (200m)']:
        results['Distance Within (200m)']['DuckDB'] = 'ERROR'
    if 'Area Weighted Interpolation' not in results or 'DuckDB' not in results['Area Weighted Interpolation']:
        results['Area Weighted Interpolation']['DuckDB'] = 'ERROR'
    if 'K-Nearest Neighbors (5)' not in results or 'DuckDB' not in results['K-Nearest Neighbors (5)']:
        results['K-Nearest Neighbors (5)']['DuckDB'] = 'ERROR'


# ============================================================================
# POSTGIS BENCHMARKS
# ============================================================================
print("\n" + "=" * 80)
print("POSTGIS BENCHMARKS")
print("=" * 80)

try:
    print("\n[1/4] Connecting to PostGIS...")
    conn = psycopg2.connect(
        host="localhost",
        database="gis",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    print("✓ Connected to PostGIS\n")

    # Query 1: Spatial Join
    print("[2/4] Query 1: Spatial Join + Aggregation")
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
    cur.fetchall()
    postgis_q1 = time.time() - start
    results['Spatial Join + Aggregation']['PostGIS'] = postgis_q1
    print(f"  Time: {postgis_q1:.3f}s")

    # Query 2: Distance Within
    print("[3/4] Query 2: Distance Within (200m)")
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
    cur.fetchall()
    postgis_q2 = time.time() - start
    results['Distance Within (200m)']['PostGIS'] = postgis_q2
    print(f"  Time: {postgis_q2:.3f}s")

    # Query 3: Area Weighted Interpolation
    print("[4/4] Query 3: Area Weighted Interpolation")
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
    cur.fetchall()
    postgis_q3 = time.time() - start
    results['Area Weighted Interpolation']['PostGIS'] = postgis_q3
    print(f"  Time: {postgis_q3:.3f}s")

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
    cur.fetchall()
    postgis_q4 = time.time() - start
    results['K-Nearest Neighbors (5)']['PostGIS'] = postgis_q4
    print(f"  Time: {postgis_q4:.3f}s")

    cur.close()
    conn.close()
    print("\n✓ PostGIS benchmarks completed")

except Exception as e:
    print(f"\n✗ PostGIS error: {e}")
    results['Spatial Join + Aggregation']['PostGIS'] = 'ERROR'
    results['Distance Within (200m)']['PostGIS'] = 'ERROR'
    results['Area Weighted Interpolation']['PostGIS'] = 'ERROR'
    results['K-Nearest Neighbors (5)']['PostGIS'] = 'ERROR'


# ============================================================================
# RESULTS SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("BENCHMARK RESULTS SUMMARY")
print("=" * 80)
print()

# Prepare table data
table_data = []
for query_name, db_results in results.items():
    row = [query_name]
    for db in ['SedonaDB', 'DuckDB', 'PostGIS']:
        value = db_results.get(db, 'N/A')
        if isinstance(value, float):
            row.append(f"{value:.3f}s")
        else:
            row.append(value)
    table_data.append(row)

headers = ['Query', 'SedonaDB', 'DuckDB', 'PostGIS']
print(tabulate(table_data, headers=headers, tablefmt='grid'))
print()

# Print speedup analysis
print("\nSPEEDUP ANALYSIS (SedonaDB vs Others):")
print("-" * 80)
for query_name, db_results in results.items():
    sedona_time = db_results.get('SedonaDB')
    duckdb_time = db_results.get('DuckDB')
    postgis_time = db_results.get('PostGIS')

    if isinstance(sedona_time, float) and isinstance(duckdb_time, float):
        speedup_duckdb = duckdb_time / sedona_time
        print(f"{query_name}:")
        print(f"  SedonaDB vs DuckDB:  {speedup_duckdb:.2f}x faster")

    if isinstance(sedona_time, float) and isinstance(postgis_time, float):
        speedup_postgis = postgis_time / sedona_time
        print(f"  SedonaDB vs PostGIS: {speedup_postgis:.2f}x faster")
    print()

print("=" * 80)
print("Benchmark completed!")
print("=" * 80)
