#!/usr/bin/env python3
"""
Load Parquet Files into PostGIS

This script loads Parquet files into PostGIS and adds:
1. Pre-transformed geometry columns (geom_4326, geom_3857)
2. GIST spatial indexes on both CRS columns

This setup enables:
- Fast spatial queries without runtime transformations
- Index-aware KNN queries using <-> operator
- Consistent comparison with other databases
"""

import geopandas as gpd
from sqlalchemy import create_engine, text
import gc


def create_db_engine():
    """Create SQLAlchemy engine for PostGIS connection"""
    return create_engine('postgresql://postgres:postgres@localhost:5432/postgres')


def load_table(engine, table_name, parquet_file):
    """
    Load a Parquet file into PostGIS table.

    Creates:
    - Base table with geometry column
    - geom_4326: WGS84 (EPSG:4326) - native CRS
    - geom_3857: Web Mercator (EPSG:3857) - for metric distances
    - GIST indexes on both geometry columns
    """
    print(f"\nLoading {table_name}...")

    # Load Parquet file
    print(f"  Reading {parquet_file}...")
    gdf = gpd.read_parquet(parquet_file)
    print(f"  ✓ Loaded {len(gdf):,} rows")

    # Write to PostGIS
    print(f"  Writing to PostGIS table '{table_name}'...")
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
    print("  ✓ Table created")

    # Add transformed geometry columns and indexes
    print("  Adding transformed geometry columns and indexes...")
    with engine.connect() as conn:
        # Add columns
        conn.execute(text(f"""
            ALTER TABLE {table_name}
            ADD COLUMN geom_4326 geometry,
            ADD COLUMN geom_3857 geometry
        """))
        conn.commit()

        # Populate transformed geometries
        # ST_MakeValid ensures geometries are valid
        conn.execute(text(f"""
            UPDATE {table_name}
            SET geom_4326 = ST_Transform(ST_MakeValid(geometry), 4326),
                geom_3857 = ST_Transform(ST_MakeValid(geometry), 3857)
        """))
        conn.commit()

        # Create GIST indexes for spatial queries
        # EPSG:3857 index: Used for metric distance queries (ST_DWithin with 200m)
        conn.execute(text(f"""
            CREATE INDEX geom_3857_idx_{table_name}
            ON {table_name} USING GIST(geom_3857)
        """))
        conn.commit()

        # EPSG:4326 index: Used for geographic intersections
        conn.execute(text(f"""
            CREATE INDEX geom_4326_idx_{table_name}
            ON {table_name} USING GIST(geom_4326)
        """))
        conn.commit()

    print("  ✓ Indexes created")

    # Clean up memory
    del gdf
    gc.collect()


def main():
    """Load all Parquet files into PostGIS"""
    print("=" * 80)
    print("Loading Parquet files into PostGIS")
    print("=" * 80)

    engine = create_db_engine()

    # Define tables to load
    tables = [
        ('buildings_parquet', 'data/buildings.parquet'),
        ('hydrants_parquet', 'data/hydrants.parquet'),
        ('neighborhoods_parquet', 'data/neighborhoods.parquet'),
        ('census_blocks_parquet', 'data/census_blocks.parquet')
    ]

    # Load each table
    for i, (table_name, file_path) in enumerate(tables, 1):
        print(f"\n[{i}/{len(tables)}] {table_name}")
        load_table(engine, table_name, file_path)

    # Summary
    print("\n" + "=" * 80)
    print("✓ All Parquet data loaded into PostGIS")
    print("=" * 80)
    print("\nTables created:")
    for table_name, _ in tables:
        print(f"  - {table_name}")
        print(f"    - geom_4326 (GIST index)")
        print(f"    - geom_3857 (GIST index)")


if __name__ == '__main__':
    main()
