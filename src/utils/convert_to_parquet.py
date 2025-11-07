#!/usr/bin/env python3
"""
Convert GeoJSON Files to Parquet Format

Converts GeoJSON files to Parquet for:
- Faster loading (30s vs >2 hours)
- Smaller files (78% reduction: 933MB → 207MB)
- Lower memory usage (avoids OOM errors)
- Better column-oriented access patterns

Required for:
- SedonaDB (GeoJSON causes OOM with >47GB RAM)
- DuckDB (49-61x faster than GeoJSON)
- PostGIS (2.3x faster than GeoJSON)
"""

import geopandas as gpd
import gc


def convert_file(input_file, output_file, description):
    """
    Convert a single GeoJSON file to Parquet format.

    Args:
        input_file: Path to input GeoJSON file
        output_file: Path to output Parquet file
        description: Human-readable description for logging
    """
    print(f"\nConverting {description}...")
    print(f"  Reading {input_file}...")

    try:
        # Read GeoJSON file
        gdf = gpd.read_file(input_file)
        print(f"  ✓ Loaded {len(gdf):,} rows")

        # Write Parquet file
        print(f"  Writing {output_file}...")
        gdf.to_parquet(output_file)
        print(f"  ✓ Saved to {output_file}")

        # Clean up memory
        del gdf
        gc.collect()

    except Exception as e:
        print(f"  ✗ Error: {e}")


def main():
    """Convert all GeoJSON files to Parquet"""
    print("=" * 80)
    print("Converting GeoJSON files to Parquet format")
    print("=" * 80)
    print("\nParquet benefits:")
    print("  - 78% smaller (933MB → 207MB)")
    print("  - Loads 240x faster (30s vs >2 hours)")
    print("  - Avoids OOM errors in SedonaDB")
    print("  - 49-61x faster in DuckDB")

    # Define files to convert
    conversions = [
        ('data/Building_Footprints.geojson', 'data/buildings.parquet', 'buildings'),
        ('data/NYCDEPCitywideHydrants.geojson', 'data/hydrants.parquet', 'hydrants'),
        ('data/nyc_hoods.geojson', 'data/neighborhoods.parquet', 'neighborhoods'),
        ('data/nys_census_blocks.geojson', 'data/census_blocks.parquet', 'census blocks')
    ]

    # Convert each file
    for i, (input_file, output_file, description) in enumerate(conversions, 1):
        print(f"\n[{i}/{len(conversions)}] {description.title()}")
        convert_file(input_file, output_file, description)

    # Summary
    print("\n" + "=" * 80)
    print("✓ Conversion complete!")
    print("=" * 80)
    print("\nParquet files created:")
    for _, output_file, description in conversions:
        print(f"  - {output_file} ({description})")


if __name__ == '__main__':
    main()
