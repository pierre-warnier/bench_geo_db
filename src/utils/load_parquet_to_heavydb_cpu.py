#!/usr/bin/env python3
"""
Load Parquet files into HeavyDB CPU with POINT geometry columns.
Uses ST_SetSRID(ST_Point(lon, lat), 4326) for proper spatial support.
Connects to CPU instance on port 6275.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from heavyai import connect
import time

DATA_DIR = 'data'

def create_heavydb_connection():
    return connect(
        user='admin',
        password='HyperInteractive',
        host='localhost',
        port=6275,  # CPU instance port
        dbname='heavyai'
    )

def load_hydrants(conn):
    """Load hydrants with POINT geometry"""
    print("\n[1/4] Loading hydrants...")
    start = time.time()

    gdf = gpd.read_parquet(f'{DATA_DIR}/hydrants.parquet')
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    print(f"  + Read {len(gdf):,} rows")

    try:
        conn.execute("DROP TABLE IF EXISTS hydrants")
    except:
        pass

    conn.execute("""
        CREATE TABLE hydrants (
            unitid TEXT,
            geom GEOMETRY(POINT, 4326)
        )
    """)

    df = pd.DataFrame()
    df['unitid'] = gdf['unitid'].astype(str) if 'unitid' in gdf.columns else [f'H{i}' for i in range(len(gdf))]
    df['lon'] = gdf.geometry.x.astype('float64')
    df['lat'] = gdf.geometry.y.astype('float64')

    conn.execute("""
        CREATE TABLE hydrants_temp (
            unitid TEXT,
            lon DOUBLE,
            lat DOUBLE
        )
    """)
    conn.load_table('hydrants_temp', df)

    conn.execute("""
        INSERT INTO hydrants (unitid, geom)
        SELECT unitid, ST_SetSRID(ST_Point(lon, lat), 4326)
        FROM hydrants_temp
    """)
    conn.execute("DROP TABLE hydrants_temp")

    result = conn.execute("SELECT COUNT(*) FROM hydrants")
    count = result.fetchone()[0]
    print(f"  + Loaded {count:,} hydrants ({time.time()-start:.2f}s)")
    return count

def load_buildings(conn):
    """Load buildings with POINT centroid geometry"""
    print("\n[2/4] Loading buildings...")
    start = time.time()

    gdf = gpd.read_parquet(f'{DATA_DIR}/buildings.parquet')
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    print(f"  + Read {len(gdf):,} rows")

    try:
        conn.execute("DROP TABLE IF EXISTS buildings")
    except:
        pass

    conn.execute("""
        CREATE TABLE buildings (
            building_id BIGINT,
            heightroof DOUBLE,
            geom GEOMETRY(POINT, 4326)
        )
    """)

    centroids = gdf.geometry.centroid

    df = pd.DataFrame()
    df['building_id'] = gdf['doitt_id'].astype('int64') if 'doitt_id' in gdf.columns else np.arange(len(gdf), dtype='int64')
    df['heightroof'] = gdf['heightroof'].fillna(0).astype('float64') if 'heightroof' in gdf.columns else 0.0
    df['lon'] = centroids.x.astype('float64')
    df['lat'] = centroids.y.astype('float64')

    conn.execute("""
        CREATE TABLE buildings_temp (
            building_id BIGINT,
            heightroof DOUBLE,
            lon DOUBLE,
            lat DOUBLE
        )
    """)
    conn.load_table('buildings_temp', df)

    conn.execute("""
        INSERT INTO buildings (building_id, heightroof, geom)
        SELECT building_id, heightroof, ST_SetSRID(ST_Point(lon, lat), 4326)
        FROM buildings_temp
    """)
    conn.execute("DROP TABLE buildings_temp")

    result = conn.execute("SELECT COUNT(*) FROM buildings")
    count = result.fetchone()[0]
    print(f"  + Loaded {count:,} buildings ({time.time()-start:.2f}s)")
    return count

def load_neighborhoods(conn):
    """Load neighborhoods with POINT centroid geometry"""
    print("\n[3/4] Loading neighborhoods...")
    start = time.time()

    gdf = gpd.read_parquet(f'{DATA_DIR}/neighborhoods.parquet')
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    print(f"  + Read {len(gdf):,} rows")

    try:
        conn.execute("DROP TABLE IF EXISTS neighborhoods")
    except:
        pass

    conn.execute("""
        CREATE TABLE neighborhoods (
            neighborhood TEXT,
            geom GEOMETRY(POINT, 4326)
        )
    """)

    name_col = None
    for col in gdf.columns:
        if col != 'geometry' and gdf[col].dtype == 'object':
            name_col = col
            break

    centroids = gdf.geometry.centroid

    df = pd.DataFrame()
    df['neighborhood'] = gdf[name_col].astype(str) if name_col else [f'N{i}' for i in range(len(gdf))]
    df['lon'] = centroids.x.astype('float64')
    df['lat'] = centroids.y.astype('float64')

    conn.execute("""
        CREATE TABLE neighborhoods_temp (
            neighborhood TEXT,
            lon DOUBLE,
            lat DOUBLE
        )
    """)
    conn.load_table('neighborhoods_temp', df)

    conn.execute("""
        INSERT INTO neighborhoods (neighborhood, geom)
        SELECT neighborhood, ST_SetSRID(ST_Point(lon, lat), 4326)
        FROM neighborhoods_temp
    """)
    conn.execute("DROP TABLE neighborhoods_temp")

    result = conn.execute("SELECT COUNT(*) FROM neighborhoods")
    count = result.fetchone()[0]
    print(f"  + Loaded {count:,} neighborhoods ({time.time()-start:.2f}s)")
    return count

def load_census_blocks(conn):
    """Load census blocks with POINT centroid geometry"""
    print("\n[4/4] Loading census_blocks...")
    start = time.time()

    gdf = gpd.read_parquet(f'{DATA_DIR}/census_blocks.parquet')
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')
    print(f"  + Read {len(gdf):,} rows")

    try:
        conn.execute("DROP TABLE IF EXISTS census_blocks")
    except:
        pass

    conn.execute("""
        CREATE TABLE census_blocks (
            block_id BIGINT,
            population DOUBLE,
            geom GEOMETRY(POINT, 4326)
        )
    """)

    pop_col = None
    for c in gdf.columns:
        if 'pop' in c.lower():
            pop_col = c
            break

    centroids = gdf.geometry.centroid

    df = pd.DataFrame()
    df['block_id'] = np.arange(len(gdf), dtype='int64')
    df['population'] = gdf[pop_col].fillna(0).astype('float64') if pop_col else 0.0
    df['lon'] = centroids.x.astype('float64')
    df['lat'] = centroids.y.astype('float64')

    conn.execute("""
        CREATE TABLE census_blocks_temp (
            block_id BIGINT,
            population DOUBLE,
            lon DOUBLE,
            lat DOUBLE
        )
    """)
    conn.load_table('census_blocks_temp', df)

    conn.execute("""
        INSERT INTO census_blocks (block_id, population, geom)
        SELECT block_id, population, ST_SetSRID(ST_Point(lon, lat), 4326)
        FROM census_blocks_temp
    """)
    conn.execute("DROP TABLE census_blocks_temp")

    result = conn.execute("SELECT COUNT(*) FROM census_blocks")
    count = result.fetchone()[0]
    print(f"  + Loaded {count:,} census blocks ({time.time()-start:.2f}s)")
    return count


def main():
    print("=" * 80)
    print("Loading Parquet files into HeavyDB CPU (port 6275)")
    print("=" * 80)

    conn = create_heavydb_connection()
    print("+ Connected to HeavyDB CPU")

    load_hydrants(conn)
    load_buildings(conn)
    load_neighborhoods(conn)
    load_census_blocks(conn)

    print("\n" + "=" * 80)
    print("+ All data loaded into HeavyDB CPU")
    print("=" * 80)

    print("\nTables created:")
    for table in ['hydrants', 'buildings', 'neighborhoods', 'census_blocks']:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = result.fetchone()[0]
        print(f"  - {table}: {count:,} rows")

if __name__ == '__main__':
    main()
