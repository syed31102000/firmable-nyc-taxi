import duckdb

con = duckdb.connect("data/warehouse/nyc_taxi.duckdb")

queries = {
    "raw_count": "select count(*) from yellow_tripdata_raw",
    "fct_count": "select count(*) from fct_trips",
    "daily_revenue_preview": "select * from agg_daily_revenue limit 5",
    "zone_perf_preview": "select * from agg_zone_performance limit 5"
}

for name, sql in queries.items():
    print(f"\n--- {name} ---")
    print(con.execute(sql).fetchdf())