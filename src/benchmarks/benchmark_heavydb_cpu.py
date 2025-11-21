#!/usr/bin/env python3
"""
HeavyDB CPU Benchmark - CPU-only version for comparison

Uses the same queries as GPU version but on CPU-only HeavyDB instance.
Connect on port 6275 (CPU) instead of 6274 (GPU).
"""

import time
from heavyai import connect


def connect_heavydb():
    return connect(
        user='admin',
        password='HyperInteractive',
        host='localhost',
        port=6275,  # CPU instance port
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
            print(f"  + {table} grid columns added")
        except Exception as e:
            if 'already exists' in str(e).lower():
                print(f"  + {table} grid columns exist")
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
    print("HEAVYDB CPU BENCHMARKS (CPU-only)")
    print("=" * 80)

    conn = connect_heavydb()
    print("+ Connected to HeavyDB CPU (port 6275)")
    results = {}

    # Setup grid columns
    setup_grid_columns(conn)

    print("\n" + "=" * 80)
    print("Running Benchmark Queries")
    print("=" * 80)

    # Query 1: Spatial Join + Aggregation (using grid equijoin)
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
    # 0.0017 degrees ~ 200m at NYC latitude
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

    # Query 4: KNN approximation
    query4 = """
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
    run_query(conn, "KNN approximation (1000 buildings)", query4, results, 'q4')

    print("\n" + "=" * 80)
    print("+ HeavyDB CPU benchmarks completed")
    print("=" * 80)

    print("\nRESULTS:")
    for key, name in [('q1', 'Spatial Join (grid)'), ('q2', 'Distance Within'),
                      ('q3', 'Pop Estimation'), ('q4', 'KNN (1000 bldgs)')]:
        val = results.get(key, 'N/A')
        if isinstance(val, float):
            print(f"  Query {key[-1]} ({name:24}): {val:.3f}s")
        else:
            print(f"  Query {key[-1]} ({name:24}): {val}")


if __name__ == '__main__':
    main()
