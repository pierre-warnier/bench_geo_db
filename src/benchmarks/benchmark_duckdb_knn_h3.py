#!/usr/bin/env python3
"""
DuckDB KNN Benchmark using H3 Spatial Index
============================================

Finds 5 nearest hydrants for each building using H3 hexagonal cells
for efficient spatial partitioning. This avoids the 135 billion distance
calculations of a naive approach.

Strategy:
1. Convert all points to H3 cells at appropriate resolution
2. For each building, get its H3 cell and k-ring neighbors
3. Only compute distances to hydrants in those neighboring cells
4. Rank by distance and take top 5

H3 Resolution Guide (for NYC):
- Resolution 8: ~461m edge length
- Resolution 9: ~174m edge length (recommended for ~200m radius)
- Resolution 10: ~66m edge length

Author: Generated with Claude Code
"""

import time
import duckdb

# Configuration
H3_RESOLUTION = 9  # ~174m edge length
K_RING_SIZE = 3    # Search own cell + 3 rings of neighbors (~870m radius)

print("=" * 80)
print("DuckDB KNN Benchmark: H3 Spatial Index Solution")
print("=" * 80)
print()
print(f"Configuration:")
print(f"  H3 Resolution: {H3_RESOLUTION} (~174m edge length)")
print(f"  K-Ring Size: {K_RING_SIZE} (own cell + {K_RING_SIZE} neighbor rings)")
print(f"  Expected search radius: ~{174 * (K_RING_SIZE + 1)}m")
print()


def initialize_duckdb():
    """Initialize DuckDB with spatial and H3 extensions"""
    print("[1/8] Initializing DuckDB with spatial and H3 extensions...")
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
    print("[2/8] Loading Parquet files into tables...")
    start = time.time()

    # Load buildings
    print("  Loading buildings...")
    con.execute("""
        CREATE TABLE buildings AS
        SELECT * FROM read_parquet('data/buildings.parquet')
    """)

    # Load hydrants
    print("  Loading hydrants...")
    con.execute("""
        CREATE TABLE hydrants AS
        SELECT * FROM read_parquet('data/hydrants.parquet')
    """)

    elapsed = time.time() - start

    # Get counts
    buildings_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    hydrants_count = con.execute("SELECT COUNT(*) FROM hydrants").fetchone()[0]

    print(f"  ✓ Loaded {buildings_count:,} buildings")
    print(f"  ✓ Loaded {hydrants_count:,} hydrants")
    print(f"  ✓ Loading time: {elapsed:.3f}s")
    print()


def extract_coordinates(con):
    """Extract lon/lat coordinates from geometries"""
    print("[3/8] Extracting coordinates from geometries...")
    start = time.time()

    # Add columns if they don't exist (DuckDB requires separate ALTER statements)
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS lon DOUBLE")
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS lat DOUBLE")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS lon DOUBLE")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS lat DOUBLE")

    # Extract coordinates (use centroids for polygons, direct coords for points)
    print("  Extracting building coordinates...")
    con.execute("""
        UPDATE buildings
        SET lon = ST_X(ST_Centroid(geometry)),
            lat = ST_Y(ST_Centroid(geometry))
    """)

    print("  Extracting hydrant coordinates...")
    con.execute("""
        UPDATE hydrants
        SET lon = ST_X(geometry),
            lat = ST_Y(geometry)
    """)

    elapsed = time.time() - start
    print(f"  ✓ Coordinates extracted in {elapsed:.3f}s")
    print()


def compute_h3_cells(con, resolution):
    """Compute H3 cells for all points at given resolution"""
    print(f"[4/8] Computing H3 cells at resolution {resolution}...")
    start = time.time()

    # Add H3 cell columns as UBIGINT (required by h3_grid_disk function)
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS h3_cell UBIGINT")
    con.execute("ALTER TABLE hydrants ADD COLUMN IF NOT EXISTS h3_cell UBIGINT")

    # Compute H3 cells for buildings
    print("  Computing building H3 cells...")
    con.execute(f"""
        UPDATE buildings
        SET h3_cell = h3_latlng_to_cell(lat, lon, {resolution})
    """)

    # Compute H3 cells for hydrants
    print("  Computing hydrant H3 cells...")
    con.execute(f"""
        UPDATE hydrants
        SET h3_cell = h3_latlng_to_cell(lat, lon, {resolution})
    """)

    elapsed = time.time() - start

    # Show cell distribution
    unique_building_cells = con.execute("""
        SELECT COUNT(DISTINCT h3_cell) FROM buildings
    """).fetchone()[0]

    unique_hydrant_cells = con.execute("""
        SELECT COUNT(DISTINCT h3_cell) FROM hydrants
    """).fetchone()[0]

    print(f"  ✓ Buildings distributed across {unique_building_cells:,} cells")
    print(f"  ✓ Hydrants distributed across {unique_hydrant_cells:,} cells")
    print(f"  ✓ H3 cells computed in {elapsed:.3f}s")
    print()


def estimate_candidates_per_building(con, k_ring_size, sample_size=1000):
    """Estimate average candidates per building for the k-ring size"""
    print(f"[5/8] Estimating candidate set size (k-ring={k_ring_size})...")

    result = con.execute(f"""
        WITH sample_buildings AS (
            SELECT h3_cell, geometry
            FROM buildings
            USING SAMPLE {sample_size} ROWS
        ),
        neighbor_cells AS (
            SELECT
                sb.h3_cell AS building_cell,
                unnest(h3_grid_disk(sb.h3_cell, {k_ring_size})) AS neighbor_cell
            FROM sample_buildings sb
        ),
        candidates AS (
            SELECT
                nc.building_cell,
                COUNT(h.unitid) AS candidate_count
            FROM neighbor_cells nc
            LEFT JOIN hydrants h ON h.h3_cell = nc.neighbor_cell
            GROUP BY nc.building_cell
        )
        SELECT
            COALESCE(MIN(candidate_count), 0) AS min_candidates,
            COALESCE(CAST(AVG(candidate_count) AS INTEGER), 0) AS avg_candidates,
            COALESCE(MAX(candidate_count), 0) AS max_candidates,
            COALESCE(CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY candidate_count) AS INTEGER), 0) AS p95_candidates
        FROM candidates
    """).fetchone()

    min_cand, avg_cand, max_cand, p95_cand = result

    # Safety check
    if avg_cand == 0:
        print(f"  ⚠️  WARNING: No candidates found! Increasing k-ring size recommended.")
        avg_cand = 1  # Prevent division by zero

    print(f"  Candidates per building (sample of {sample_size}):")
    print(f"    Min: {min_cand:,}")
    print(f"    Avg: {avg_cand:,}")
    print(f"    P95: {p95_cand:,}")
    print(f"    Max: {max_cand:,}")

    # Estimate total comparisons
    buildings_count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    total_comparisons = buildings_count * avg_cand

    naive_comparisons = buildings_count * con.execute("SELECT COUNT(*) FROM hydrants").fetchone()[0]
    reduction_factor = naive_comparisons / total_comparisons

    print(f"  Estimated total comparisons: {total_comparisons:,}")
    print(f"  Naive approach would be: {naive_comparisons:,}")
    print(f"  Reduction factor: {reduction_factor:.1f}x")
    print()

    return avg_cand


def run_knn_query(con, k_ring_size, k=5):
    """
    Run the KNN query using H3 spatial index.

    Algorithm:
    1. For each building, get its H3 cell
    2. Expand to k-ring of neighboring cells
    3. Join to hydrants in those cells only
    4. Compute exact spheroidal distance (meters)
    5. Rank and take top K nearest
    """
    print(f"[6/8] Running KNN query (K={k}, k-ring={k_ring_size})...")
    print("  This finds the 5 nearest hydrants for each of 1.2M buildings...")
    print()

    start = time.time()

    # Create the KNN results table
    con.execute(f"""
        CREATE OR REPLACE TABLE building_hydrant_knn AS
        WITH neighbor_cells AS (
            -- For each building, get its H3 cell and k-ring neighbors
            SELECT
                b.bin AS building_id,
                b.geometry AS building_geom,
                unnest(h3_grid_disk(b.h3_cell, {k_ring_size})) AS neighbor_cell
            FROM buildings b
        ),
        candidate_pairs AS (
            -- Join buildings to hydrants within neighboring cells
            -- Use centroids for polygon buildings
            SELECT
                nc.building_id,
                h.unitid AS hydrant_id,
                ST_Distance_Spheroid(ST_Centroid(nc.building_geom), h.geometry) AS dist_m
            FROM neighbor_cells nc
            JOIN hydrants h ON h.h3_cell = nc.neighbor_cell
        ),
        ranked AS (
            -- Rank hydrants by distance for each building
            SELECT
                building_id,
                hydrant_id,
                dist_m,
                ROW_NUMBER() OVER (
                    PARTITION BY building_id
                    ORDER BY dist_m
                ) AS rn
            FROM candidate_pairs
        )
        -- Take top K nearest for each building
        SELECT
            building_id,
            hydrant_id,
            dist_m
        FROM ranked
        WHERE rn <= {k}
    """)

    elapsed = time.time() - start

    # Get result statistics
    total_results = con.execute("SELECT COUNT(*) FROM building_hydrant_knn").fetchone()[0]

    print(f"  ✓ KNN query completed in {elapsed:.3f}s")
    print(f"  ✓ Generated {total_results:,} building-hydrant pairs")
    print()

    return elapsed


def validate_results(con, expected_k=5):
    """Validate that every building has exactly K nearest hydrants"""
    print(f"[7/8] Validating results...")

    # Check if any building has fewer than K hydrants
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
        print(f"     Consider increasing K_RING_SIZE to {K_RING_SIZE + 1}")
    else:
        print(f"  ✓ All buildings have exactly {expected_k} nearest hydrants")

    print()

    return (buildings_with_less or 0) == 0


def show_sample_results(con, num_samples=5):
    """Show sample results for verification"""
    print(f"[8/8] Sample results (first {num_samples} buildings):")
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

        # Estimate candidate set size
        avg_candidates = estimate_candidates_per_building(con, K_RING_SIZE)

        # Run KNN query
        query_time = run_knn_query(con, K_RING_SIZE, k=5)

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
        print(f"K-Ring Size: {K_RING_SIZE}")
        print(f"Avg candidates per building: {avg_candidates:,}")
        print(f"Results valid: {'✓ YES' if is_valid else '✗ NO - increase K_RING_SIZE'}")
        print("=" * 80)

        con.close()

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
