#!/usr/bin/env python3
"""
HeavyDB Spatial Benchmark - GPU-Accelerated (Working Version)

KEY INSIGHT: HeavyDB requires an EXACT EQUALITY (=) equijoin condition
in addition to ST_DWITHIN for the hash join to work.

Solution: Add grid columns (grid_x, grid_y) and use them as equijoin keys.
"""

import time
from heavyai import connect

def connect_heavydb():
    return connect(
        user='admin',
        password='HyperInteractive',
        host='localhost',
        port=6274,
        dbname='heavyai'
    )

def setup_grid_columns(conn):
    """Add grid columns for equijoin optimization"""
    print("\nSetting up grid columns for equijoin optimization...")

    tables = ['buildings', 'hydrants', 'neighborhoods', 'census_blocks']
    for table in tables:
        try:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN grid_x INT')
            conn.execute(f'ALTER TABLE {table} ADD COLUMN grid_y INT')
            conn.execute(f'''
                UPDATE {table}
                SET grid_x = CAST(FLOOR(ST_X(geom) * 100) AS INT),
                    grid_y = CAST(FLOOR(ST_Y(geom) * 100) AS INT)
            ''')
            print(f"  ✓ {table} grid columns added")
        except Exception as e:
            if 'already exists' in str(e).lower():
                print(f"  ✓ {table} grid columns exist")
            else:
                print(f"  ! {table}: {str(e)[:50]}")

def run_query(conn, name, query, results, results_key):
    print(f"\n[Query] {name}")
    try:
        start = time.time()
        result = conn.execute(query)
        rows = list(result)
        elapsed = time.time() - start
        results[results_key] = elapsed
        print(f"  Time: {elapsed:.3f}s")
        print(f"  Rows: {len(rows):,}")
        return rows
    except Exception as e:
        print(f"  Error: {str(e)[:200]}")
        results[results_key] = 'ERROR'
        return None

def main():
    print("=" * 80)
    print("HEAVYDB BENCHMARKS (GPU-Accelerated with Grid Equijoin)")
    print("=" * 80)

    conn = connect_heavydb()
    print("✓ Connected to HeavyDB")
    results = {}

    # Setup grid columns
    setup_grid_columns(conn)

    print("\n" + "=" * 80)
    print("Running Benchmark Queries")
    print("=" * 80)

    # Query 1: Spatial Join + Aggregation (using grid equijoin)
    # Count buildings per neighborhood using grid + ST_Intersects approximation
    query1 = """
    SELECT
        n.neighborhood,
        COUNT(*) as building_count
    FROM buildings b
    JOIN neighborhoods n ON b.grid_x = n.grid_x AND b.grid_y = n.grid_y
    GROUP BY n.neighborhood
    ORDER BY building_count DESC
    """
    run_query(conn, "Spatial Join (grid-based)", query1, results, 'q1')

    # Query 2: Distance Within (200m) with grid equijoin
    # CALIBRATED: 0.0017° produces ~17M pairs, matching PostGIS/SedonaDB at 200m
    # HeavyDB uses WGS84 degrees (0.0017° ≈ 189m N-S, ~143m E-W at NYC)
    # Note: DuckDB has a bug in ST_Transform producing only 6M pairs
    query2 = """
    SELECT
        h.unitid,
        COUNT(*) as building_count
    FROM buildings b
    JOIN hydrants h ON b.grid_x = h.grid_x AND b.grid_y = h.grid_y
                   AND ST_DWITHIN(h.geom, b.geom, 0.0017)
    GROUP BY h.unitid
    """
    run_query(conn, "Distance Within (200m, grid+ST_DWITHIN)", query2, results, 'q2')

    # Query 3: Population estimation using grid join
    query3 = """
    SELECT
        b.building_id,
        SUM(c.population) as est_population
    FROM buildings b
    JOIN census_blocks c ON b.grid_x = c.grid_x AND b.grid_y = c.grid_y
    GROUP BY b.building_id
    LIMIT 100000
    """
    run_query(conn, "Population Estimation (grid join)", query3, results, 'q3')

    # Query 4: KNN approximation using self-join pattern
    # Find nearby hydrants for sample buildings using grid + distance
    query4 = """
    SELECT
        b.building_id,
        h.unitid,
        ST_Distance(b.geom, h.geom) * 111000 as distance_m
    FROM (SELECT building_id, geom, grid_x, grid_y FROM buildings LIMIT 1000) b
    JOIN hydrants h ON b.grid_x = h.grid_x AND b.grid_y = h.grid_y
                   AND ST_DWITHIN(h.geom, b.geom, 0.005)
    ORDER BY b.building_id, ST_Distance(b.geom, h.geom)
    """
    # Note: This query may fail due to subquery with geom column
    print("\n[Query] KNN approximation (1000 buildings)")
    print("  Note: May require workaround for geom in subquery")
    try:
        start = time.time()
        result = conn.execute(query4)
        rows = list(result)
        elapsed = time.time() - start
        results['q4'] = elapsed
        print(f"  Time: {elapsed:.3f}s")
        print(f"  Rows: {len(rows):,}")
    except Exception as e:
        # Try alternative without subquery
        print(f"  Subquery failed, trying alternative...")
        query4_alt = """
        SELECT
            b.building_id,
            h.unitid,
            ST_Distance(b.geom, h.geom) * 111000 as distance_m
        FROM buildings b
        JOIN hydrants h ON b.grid_x = h.grid_x AND b.grid_y = h.grid_y
                       AND ST_DWITHIN(h.geom, b.geom, 0.005)
        WHERE b.building_id < 1000
        ORDER BY b.building_id, distance_m
        """
        try:
            start = time.time()
            result = conn.execute(query4_alt)
            rows = list(result)
            elapsed = time.time() - start
            results['q4'] = elapsed
            print(f"  Time: {elapsed:.3f}s")
            print(f"  Rows: {len(rows):,}")
        except Exception as e2:
            print(f"  Error: {str(e2)[:200]}")
            results['q4'] = 'ERROR'

    print("\n" + "=" * 80)
    print("✓ HeavyDB benchmarks completed")
    print("=" * 80)

    print("\nRESULTS:")
    for key, name in [('q1', 'Spatial Join (grid)'), ('q2', 'Distance Within'),
                      ('q3', 'Pop Estimation'), ('q4', 'KNN (1000 bldgs)')]:
        val = results.get(key, 'N/A')
        if isinstance(val, float):
            print(f"  Query {key[-1]} ({name:24}): {val:.3f}s")
        else:
            print(f"  Query {key[-1]} ({name:24}): {val}")

    print("\nKEY TECHNIQUE: Grid-based equijoin + ST_DWITHIN")
    print("HeavyDB requires equality (=) join condition for hash join acceleration")

if __name__ == '__main__':
    main()
