#!/usr/bin/env python3
"""
SedonaDB Spatial Benchmark - Parquet Format

This benchmark runs 4 spatial queries on SedonaDB (Apache Sedona):
1. Spatial Join + Aggregation
2. Distance Within (200m buffer)
3. Area Weighted Interpolation
4. K-Nearest Neighbors (using ST_KNN function)

Note: SedonaDB requires Parquet format. GeoJSON causes OOM (>47GB RAM).
Parquet is 78% smaller (828MB → 177MB) and loads in ~30s vs >2 hours.
"""

import time
import geopandas as gpd
import sedona.db
import gc


def initialize_sedona():
    """Initialize SedonaDB connection"""
    print("\nInitializing SedonaDB...")
    sd = sedona.db.connect()
    print("✓ SedonaDB initialized\n")
    return sd


def load_parquet_data():
    """
    Load Parquet files into GeoPandas DataFrames.

    SedonaDB benefits from Parquet's columnar format:
    - 78% smaller than GeoJSON (828MB → 177MB)
    - Loads in ~30 seconds vs >2 hours for GeoJSON
    - Avoids OOM errors (GeoJSON requires >47GB RAM)
    """
    print("Loading data from Parquet files...")

    datasets = [
        ('buildings', 'data/buildings.parquet'),
        ('hydrants', 'data/hydrants.parquet'),
        ('neighborhoods', 'data/neighborhoods.parquet'),
        ('census_blocks', 'data/census_blocks.parquet')
    ]

    data = {}
    for i, (name, path) in enumerate(datasets, 1):
        print(f"[{i}/{len(datasets)}] Loading {name}...")
        df = gpd.read_parquet(path)
        data[name] = df
        print(f"  ✓ Loaded {len(df):,} rows")

    return data


def create_sedona_views(sd, data):
    """Create SedonaDB views from GeoPandas DataFrames"""
    print("\nCreating SedonaDB DataFrames and views...")

    views = {}
    for name, df in data.items():
        sedona_df = sd.create_data_frame(df)
        view_name = f"{name}_db"
        sedona_df.to_view(view_name)
        views[name] = sedona_df

    print("  ✓ All views created")

    # Clean up to save memory
    for df in data.values():
        del df
    gc.collect()

    return views


def run_query(sd, name, query, results_key, results):
    """Execute a spatial query and record timing"""
    print(f"\n[Query] {name}")
    try:
        start = time.time()
        result = sd.sql(query)
        result.execute()
        elapsed = time.time() - start
        results[results_key] = elapsed
        print(f"  Time: {elapsed:.3f}s")
        del result
        gc.collect()
    except Exception as e:
        print(f"  Error: {str(e)[:200]}")
        results[results_key] = 'ERROR'


def main():
    """Run all SedonaDB spatial benchmarks"""
    print("=" * 80)
    print("SEDONADB BENCHMARKS (Parquet Format)")
    print("=" * 80)

    # Initialize
    sd = initialize_sedona()
    results = {}

    # Load data and create views
    data = load_parquet_data()
    create_sedona_views(sd, data)

    print("\n" + "=" * 80)
    print("Running Benchmark Queries")
    print("=" * 80)

    # Query 1: Spatial Join + Aggregation
    # Count buildings per neighborhood using ST_Intersects
    query1 = """
    SELECT
        COUNT(b.bin) as building_count,
        n.neighborhood
    FROM neighborhoods_db n
    JOIN buildings_db b
        ON ST_Intersects(b.geometry, n.geometry)
    GROUP BY n.neighborhood
    """
    run_query(sd, "Spatial Join + Aggregation", query1, 'q1', results)

    # Query 2: Distance Within (200m)
    # Count buildings within 200m of each hydrant
    # Runtime transformation to EPSG:3857 for metric distances
    query2 = """
    SELECT
        COUNT(b.bin) as building_count,
        h.unitid
    FROM hydrants_db h
    JOIN buildings_db b
        ON ST_DWithin(
            ST_Transform(b.geometry, '3857'),
            ST_Transform(h.geometry, '3857'),
            200)
    GROUP BY h.unitid
    """
    run_query(sd, "Distance Within (200m)", query2, 'q2', results)

    # Query 3: Area Weighted Interpolation
    # Estimate population per building using census block overlap
    query3 = """
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
    FROM census_blocks_db c
    JOIN buildings_db b
        ON ST_Intersects(c.geometry, b.geometry)
    GROUP BY b.bin
    """
    run_query(sd, "Area Weighted Interpolation", query3, 'q3', results)

    # Query 4: K-Nearest Neighbors
    # Find 5 nearest hydrants for each building using ST_KNN
    # SedonaDB's ST_KNN uses optimized spatial partitioning
    # Much faster than PostGIS GIST index (0.77s vs 44s)
    query4 = """
    SELECT
        b.bin as building_id,
        h.unitid as hydrant_id,
        ST_Distance(b.geometry, h.geometry) as distance
    FROM hydrants_db h
    JOIN buildings_db b
        ON ST_KNN(b.geometry, h.geometry, 5, true)
    """
    run_query(sd, "K-Nearest Neighbors (5)", query4, 'q4', results)

    # Print results
    print("\n" + "=" * 80)
    print("✓ SedonaDB benchmarks completed")
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


if __name__ == '__main__':
    main()
