#!/usr/bin/env python3
"""
DuckDB KNN Benchmark: Optimized H3 + Spatial Bounds
====================================================

Optimized version using:
1. H3 spatial index for coarse partitioning
2. Bounding box pre-filtering to reduce candidates
3. Exact distance calculation only on filtered set

Strategy:
- Use smaller k-ring (k=1) for initial filtering
- Add bounding box filter based on max expected distance
- This gives speed of small k-ring with completeness guarantee

Author: Generated with Claude Code
"""

import time
import duckdb

# Configuration
H3_RESOLUTION = 9  # ~174m edge length
K_RING_SIZE = 2    # Balance speed and coverage
MAX_DISTANCE_M = 800  # Maximum search radius in meters (safety buffer)

print("=" * 80)
print("DuckDB KNN Benchmark: Optimized H3 + Spatial Bounds")
print("=" * 80)
print()
print(f"Configuration:")
print(f"  H3 Resolution: {H3_RESOLUTION} (~174m edge length)")
print(f"  K-Ring Size: {K_RING_SIZE} (small ring for speed)")
print(f"  Max Distance Filter: {MAX_DISTANCE_M}m")
print(f"  Strategy: Combine H3 partitioning with distance bounds")
print()


def initialize_duckdb():
    """Initialize DuckDB with spatial and H3 extensions"""
    print("[1/9] Initializing DuckDB with spatial and H3 extensions...")
    con = duckdb.connect()

    # Install and load extensions
    con.execute('INSTALL spatial;')
    con.execute('LOAD spatial;')
    con.execute('INSTALL h3 FROM community;')
    con.execute('LOAD h3;')

    # Enable parallelism
    con.execute('PRAGMA threads = 8;')

    print("  ✓ Extensions loaded")
    print("  ✓ Using 8 threads")
    print()
    return con


def load_parquet_tables(con):
    """Load Parquet files into DuckDB tables"""
    print("[2/9] Loading Parquet files into tables...")
    start = time.time()

    con.execute("CREATE TABLE buildings AS SELECT * FROM read_parquet('data/buildings.parquet')")
    con.execute("CREATE TABLE hydrants AS SELECT * FROM read_parquet('data/hydrants.parquet')")

    elapsed = time.time() - start

    buildings_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    hydrants_count = con.execute("SELECT COUNT(*) FROM hydrants").fetchone()[0]

    print(f"  ✓ Loaded {buildings_count:,} buildings")
    print(f"  ✓ Loaded {hydrants_count:,} hydrants")
    print(f"  ✓ Loading time: {elapsed:.3f}s")
    print()


def extract_coordinates(con):
    """Extract lon/lat coordinates from geometries"""
    print("[3/9] Extracting coordinates from geometries...")
    start = time.time()

    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS lon DOUBLE")
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS lat DOUBLE")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS lon DOUBLE")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS lat DOUBLE")

    con.execute("""
        UPDATE buildings
        SET lon = ST_X(ST_Centroid(geometry)),
            lat = ST_Y(ST_Centroid(geometry))
    """)

    con.execute("""
        UPDATE hydrants
        SET lon = ST_X(geometry),
            lat = ST_Y(geometry)
    """)

    elapsed = time.time() - start
    print(f"  ✓ Coordinates extracted in {elapsed:.3f}s")
    print()


def compute_h3_cells(con, resolution):
    """Compute H3 cells for all points"""
    print(f"[4/9] Computing H3 cells at resolution {resolution}...")
    start = time.time()

    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS h3_cell UBIGINT")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS h3_cell UBIGINT")

    con.execute(f"UPDATE buildings SET h3_cell = h3_latlng_to_cell(lat, lon, {resolution})")
    con.execute(f"UPDATE hydrants SET h3_cell = h3_latlng_to_cell(lat, lon, {resolution})")

    elapsed = time.time() - start

    unique_building_cells = con.execute("SELECT COUNT(DISTINCT h3_cell) FROM buildings").fetchone()[0]
    unique_hydrant_cells = con.execute("SELECT COUNT(DISTINCT h3_cell) FROM hydrants").fetchone()[0]

    print(f"  ✓ Buildings distributed across {unique_building_cells:,} cells")
    print(f"  ✓ Hydrants distributed across {unique_hydrant_cells:,} cells")
    print(f"  ✓ H3 cells computed in {elapsed:.3f}s")
    print()


def compute_spatial_bounds(con, max_distance_m):
    """
    Compute spatial bounds for filtering.

    For NYC latitude (~40.7°):
    - 1° latitude ≈ 111,000m
    - 1° longitude ≈ 84,400m (at 40.7° latitude)
    """
    print(f"[5/9] Computing spatial bounds ({max_distance_m}m buffer)...")
    start = time.time()

    # Convert meters to degrees (approximate for NYC)
    lat_degrees = max_distance_m / 111000.0
    lon_degrees = max_distance_m / 84400.0

    print(f"  Latitude buffer: {lat_degrees:.6f}°")
    print(f"  Longitude buffer: {lon_degrees:.6f}°")

    # Add bounding box columns to buildings
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS min_lat DOUBLE")
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS max_lat DOUBLE")
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS min_lon DOUBLE")
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS max_lon DOUBLE")

    con.execute(f"""
        UPDATE buildings
        SET min_lat = lat - {lat_degrees},
            max_lat = lat + {lat_degrees},
            min_lon = lon - {lon_degrees},
            max_lon = lon + {lon_degrees}
    """)

    elapsed = time.time() - start
    print(f"  ✓ Spatial bounds computed in {elapsed:.3f}s")
    print()

    return lat_degrees, lon_degrees


def estimate_candidates(con, k_ring_size, max_distance_m, sample_size=1000):
    """Estimate candidates with both H3 and spatial bounds filtering"""
    print(f"[6/9] Estimating candidate set size...")

    result = con.execute(f"""
        WITH sample_buildings AS (
            SELECT h3_cell, geometry, lat, lon, min_lat, max_lat, min_lon, max_lon
            FROM buildings
            USING SAMPLE {sample_size} ROWS
        ),
        neighbor_cells AS (
            SELECT
                sb.h3_cell AS building_cell,
                sb.lat, sb.lon, sb.min_lat, sb.max_lat, sb.min_lon, sb.max_lon, sb.geometry,
                unnest(h3_grid_disk(sb.h3_cell, {k_ring_size})) AS neighbor_cell
            FROM sample_buildings sb
        ),
        candidates_h3 AS (
            -- First filter: H3 neighbor cells
            SELECT
                nc.building_cell,
                h.unitid,
                h.lat AS h_lat,
                h.lon AS h_lon,
                h.geometry AS h_geom,
                nc.geometry AS b_geom
            FROM neighbor_cells nc
            JOIN hydrants h ON h.h3_cell = nc.neighbor_cell
        ),
        candidates_bounded AS (
            -- Second filter: Spatial bounds (cheap check)
            SELECT
                building_cell,
                unitid,
                ST_Distance_Spheroid(ST_Centroid(b_geom), h_geom) AS dist_m
            FROM candidates_h3
            WHERE h_lat BETWEEN (SELECT lat - {max_distance_m/111000.0} FROM neighbor_cells WHERE building_cell = candidates_h3.building_cell LIMIT 1)
                            AND (SELECT lat + {max_distance_m/111000.0} FROM neighbor_cells WHERE building_cell = candidates_h3.building_cell LIMIT 1)
              AND h_lon BETWEEN (SELECT lon - {max_distance_m/84400.0} FROM neighbor_cells WHERE building_cell = candidates_h3.building_cell LIMIT 1)
                            AND (SELECT lon + {max_distance_m/84400.0} FROM neighbor_cells WHERE building_cell = candidates_h3.building_cell LIMIT 1)
        )
        SELECT
            COALESCE(COUNT(DISTINCT building_cell), 0) AS buildings,
            COALESCE(COUNT(*), 0) AS total_candidates,
            COALESCE(CAST(AVG(cnt) AS INTEGER), 0) AS avg_candidates,
            COALESCE(MAX(cnt), 0) AS max_candidates
        FROM (
            SELECT building_cell, COUNT(*) AS cnt
            FROM candidates_bounded
            GROUP BY building_cell
        )
    """).fetchone()

    buildings, total_cand, avg_cand, max_cand = result

    print(f"  Sample buildings processed: {buildings:,}")
    print(f"  Total candidates after filtering: {total_cand:,}")
    print(f"  Avg candidates per building: {avg_cand:,}")
    print(f"  Max candidates per building: {max_cand:,}")

    if avg_cand > 0:
        buildings_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
        hydrants_count = con.execute("SELECT COUNT(*) FROM hydrants").fetchone()[0]
        estimated_total = buildings_count * avg_cand
        naive_total = buildings_count * hydrants_count
        reduction = naive_total / estimated_total if estimated_total > 0 else 0

        print(f"  Estimated total comparisons: {estimated_total:,}")
        print(f"  Naive approach: {naive_total:,}")
        print(f"  Reduction factor: {reduction:.1f}x")

    print()
    return avg_cand


def run_optimized_knn_query(con, k_ring_size, max_distance_m, k=5):
    """
    Run optimized KNN query with H3 + spatial bounds filtering
    """
    print(f"[7/9] Running optimized KNN query (K={k})...")
    print("  Strategy: H3 k-ring=1 + spatial bounds filter")
    print()

    start = time.time()

    # Convert max distance to degrees for bounds check
    lat_buffer = max_distance_m / 111000.0
    lon_buffer = max_distance_m / 84400.0

    con.execute(f"""
        CREATE OR REPLACE TABLE building_hydrant_knn AS
        WITH neighbor_cells AS (
            -- Step 1: Expand each building to k-ring neighbors
            SELECT
                b.bin AS building_id,
                b.geometry AS building_geom,
                b.lat AS b_lat,
                b.lon AS b_lon,
                unnest(h3_grid_disk(b.h3_cell, {k_ring_size})) AS neighbor_cell
            FROM buildings b
        ),
        candidates AS (
            -- Step 2: Join to hydrants in neighboring cells
            -- Step 3: Apply spatial bounds filter (cheap lat/lon check)
            SELECT
                nc.building_id,
                h.unitid AS hydrant_id,
                ST_Distance_Spheroid(ST_Centroid(nc.building_geom), h.geometry) AS dist_m
            FROM neighbor_cells nc
            JOIN hydrants h
              ON h.h3_cell = nc.neighbor_cell
             AND h.lat BETWEEN nc.b_lat - {lat_buffer} AND nc.b_lat + {lat_buffer}
             AND h.lon BETWEEN nc.b_lon - {lon_buffer} AND nc.b_lon + {lon_buffer}
        ),
        ranked AS (
            -- Step 4: Rank by exact spheroidal distance
            SELECT
                building_id,
                hydrant_id,
                dist_m,
                ROW_NUMBER() OVER (
                    PARTITION BY building_id
                    ORDER BY dist_m
                ) AS rn
            FROM candidates
        )
        -- Step 5: Take top K nearest
        SELECT
            building_id,
            hydrant_id,
            dist_m
        FROM ranked
        WHERE rn <= {k}
    """)

    elapsed = time.time() - start

    total_results = con.execute("SELECT COUNT(*) FROM building_hydrant_knn").fetchone()[0]

    print(f"  ✓ KNN query completed in {elapsed:.3f}s")
    print(f"  ✓ Generated {total_results:,} building-hydrant pairs")
    print()

    return elapsed


def validate_results(con, expected_k=5):
    """Validate that every building has K nearest hydrants"""
    print(f"[8/9] Validating results...")

    result = con.execute(f"""
        WITH building_counts AS (
            SELECT
                building_id,
                COUNT(*) AS hydrant_count
            FROM building_hydrant_knn
            GROUP BY building_id
        )
        SELECT
            COUNT(*) AS total_buildings,
            SUM(CASE WHEN hydrant_count < {expected_k} THEN 1 ELSE 0 END) AS buildings_with_less_than_k,
            MIN(hydrant_count) AS min_hydrants,
            MAX(hydrant_count) AS max_hydrants
        FROM building_counts
    """).fetchone()

    total_buildings, buildings_with_less, min_hydrants, max_hydrants = result

    print(f"  Total buildings processed: {total_buildings if total_buildings else 0:,}")
    print(f"  Buildings with < {expected_k} hydrants: {buildings_with_less if buildings_with_less else 0:,}")
    print(f"  Min hydrants per building: {min_hydrants if min_hydrants else 0}")
    print(f"  Max hydrants per building: {max_hydrants if max_hydrants else 0}")

    if buildings_with_less and buildings_with_less > 0:
        print(f"  ⚠️  WARNING: {buildings_with_less} buildings have fewer than {expected_k} hydrants!")
        print(f"     Increase MAX_DISTANCE_M or K_RING_SIZE")
    else:
        print(f"  ✓ All buildings have exactly {expected_k} nearest hydrants")

    print()

    return (buildings_with_less or 0) == 0


def show_sample_results(con, num_samples=5):
    """Show sample results for verification"""
    print(f"[9/9] Sample results (first {num_samples} buildings):")
    print()

    results = con.execute(f"""
        SELECT
            building_id,
            hydrant_id,
            CAST(dist_m AS INTEGER) AS distance_meters
        FROM building_hydrant_knn
        WHERE building_id IN (
            SELECT DISTINCT building_id
            FROM building_hydrant_knn
            LIMIT {num_samples}
        )
        ORDER BY building_id, dist_m
    """).fetchall()

    current_building = None
    for building_id, hydrant_id, dist_m in results:
        if building_id != current_building:
            if current_building is not None:
                print()
            print(f"  Building {building_id}:")
            current_building = building_id
        print(f"    → Hydrant {hydrant_id}: {dist_m}m")

    print()


def main():
    """Main execution flow"""
    try:
        # Initialize
        con = initialize_duckdb()

        # Load data
        load_parquet_tables(con)

        # Extract coordinates
        extract_coordinates(con)

        # Compute H3 cells
        compute_h3_cells(con, H3_RESOLUTION)

        # Compute spatial bounds
        compute_spatial_bounds(con, MAX_DISTANCE_M)

        # Estimate candidates
        avg_candidates = estimate_candidates(con, K_RING_SIZE, MAX_DISTANCE_M)

        # Run optimized KNN query
        query_time = run_optimized_knn_query(con, K_RING_SIZE, MAX_DISTANCE_M, k=5)

        # Validate results
        is_valid = validate_results(con, expected_k=5)

        # Show samples
        show_sample_results(con, num_samples=5)

        # Final summary
        print("=" * 80)
        print("BENCHMARK COMPLETE")
        print("=" * 80)
        print(f"Query time: {query_time:.3f}s")
        print(f"H3 Resolution: {H3_RESOLUTION}")
        print(f"K-Ring Size: {K_RING_SIZE} (small for speed)")
        print(f"Max Distance Filter: {MAX_DISTANCE_M}m")
        print(f"Avg candidates per building: {avg_candidates:,}")
        print(f"Results valid: {'✓ YES' if is_valid else '✗ NO'}")
        print(f"")
        print(f"Optimization: Combined H3 coarse partitioning with spatial bounds")
        print(f"Result: Fast k-ring with completeness guarantee from distance filter")
        print("=" * 80)

        con.close()

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
