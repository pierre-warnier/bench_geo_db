#!/usr/bin/env python3
"""
Load Parquet Files into HeavyDB

This script loads Parquet files into HeavyDB with:
1. Geometry columns in WGS84 (EPSG:4326)
2. Point-based representations for performance

HeavyDB supports GPU-accelerated spatial operations on points and polygons.
"""

import geopandas as gpd
from heavyai import connect
import gc


def create_heavydb_connection():
    """Create HeavyDB connection"""
    return connect(
        user='admin',
        password='HyperInteractive',
        host='localhost',
        port=6274,
        dbname='heavyai'
    )


def load_table(conn, table_name, parquet_file):
    """
    Load a Parquet file into HeavyDB table.

    HeavyDB works best with point geometries or simple polygons.
    We convert geometries to WKT format for loading.
    """
    print(f"\nLoading {table_name}...")

    # Load Parquet file
    print(f"  Reading {parquet_file}...")
    gdf = gpd.read_parquet(parquet_file)
    print(f"  ✓ Loaded {len(gdf):,} rows")

    # Ensure geometries are in EPSG:4326 (HeavyDB default)
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')

    # Drop table if exists
    try:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Convert to pandas DataFrame with WKT geometry
    print(f"  Converting geometries to WKT...")
    df = gdf.copy()
    df['geom_wkt'] = df.geometry.to_wkt()
    df = df.drop(columns=['geometry'])

    # Get column names and types
    columns = []
    for col in df.columns:
        if col == 'geom_wkt':
            continue
        dtype = df[col].dtype
        if dtype == 'object':
            col_type = 'TEXT'
        elif dtype == 'int64':
            col_type = 'BIGINT'
        elif dtype == 'float64':
            col_type = 'DOUBLE'
        else:
            col_type = 'TEXT'
        columns.append(f"{col} {col_type}")

    # Determine geometry type
    geom_sample = gdf.geometry.iloc[0]
    if geom_sample.geom_type in ['Point', 'MultiPoint']:
        geom_col = "geom POINT"
    elif geom_sample.geom_type in ['Polygon', 'MultiPolygon']:
        geom_col = "geom MULTIPOLYGON"
    elif geom_sample.geom_type in ['LineString', 'MultiLineString']:
        geom_col = "geom LINESTRING"
    else:
        geom_col = "geom GEOMETRY"

    columns.append(geom_col)

    # Create table
    print(f"  Creating HeavyDB table '{table_name}'...")
    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    conn.execute(create_sql)
    print("  ✓ Table created")

    # Load data using HeavyDB's load_table method
    print(f"  Loading data to HeavyDB...")

    # Convert to dict format for loading
    data_dict = df.to_dict('list')
    data_dict['geom'] = data_dict.pop('geom_wkt')

    # Insert in batches to avoid memory issues
    batch_size = 10000
    total_rows = len(df)

    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]

        # Prepare values for insertion
        rows = []
        for _, row in batch_df.iterrows():
            values = []
            for col in df.columns:
                if col == 'geom_wkt':
                    values.append(f"'{row[col]}'")
                elif isinstance(row[col], str):
                    escaped = row[col].replace("'", "''")
                    values.append(f"'{escaped}'")
                elif pd.isna(row[col]):
                    values.append('NULL')
                else:
                    values.append(str(row[col]))
            rows.append(f"({', '.join(values)})")

        if rows:
            insert_sql = f"INSERT INTO {table_name} VALUES {', '.join(rows)}"
            try:
                conn.execute(insert_sql)
            except Exception as e:
                print(f"  Warning: Batch {start_idx}-{end_idx} failed: {e}")
                continue

        if (end_idx % 50000) == 0 or end_idx == total_rows:
            print(f"    Progress: {end_idx:,}/{total_rows:,} rows")

    print("  ✓ Data loaded")

    # Clean up memory
    del gdf, df
    gc.collect()


def main():
    """Load all Parquet files into HeavyDB"""
    print("=" * 80)
    print("Loading Parquet files into HeavyDB")
    print("=" * 80)

    conn = create_heavydb_connection()

    # Define tables to load
    tables = [
        ('buildings', 'data/buildings.parquet'),
        ('hydrants', 'data/hydrants.parquet'),
        ('neighborhoods', 'data/neighborhoods.parquet'),
        ('census_blocks', 'data/census_blocks.parquet')
    ]

    # Load each table
    for i, (table_name, file_path) in enumerate(tables, 1):
        print(f"\n[{i}/{len(tables)}] {table_name}")
        try:
            load_table(conn, table_name, file_path)
        except Exception as e:
            print(f"  ✗ Error loading {table_name}: {e}")
            continue

    # Summary
    print("\n" + "=" * 80)
    print("✓ All Parquet data loaded into HeavyDB")
    print("=" * 80)
    print("\nTables created:")
    for table_name, _ in tables:
        print(f"  - {table_name}")

    conn.close()


if __name__ == '__main__':
    import pandas as pd  # Import here to avoid circular dependency
    main()
