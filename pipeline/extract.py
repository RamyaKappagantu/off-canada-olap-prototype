import duckdb

DB_PATH = 'off.duckdb'
CANADA_PARQUET = 'data/raw/off-canada.parquet'
def extract_phase():
    try:
        with duckdb.connect(DB_PATH) as conn:
            print("Connected")
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
            print(f"Loading {CANADA_PARQUET} into raw_layer.canada_parquet!!")
            conn.execute(f"""CREATE OR REPLACE TABLE raw_layer.canada_parquet AS
                    SELECT * FROM read_parquet('{CANADA_PARQUET}')""")
    
            #Verify whether the table has been created
            data_count = conn.execute("SELECT COUNT(*) FROM raw_layer.canada_parquet").fetchone()[0]
            print(f"Loaded {data_count} rows from the parquet file")

            # Other data sources can be added here
        return True
    except duckdb.Error as e:
        print(f"Duckdb error during EL phase: {e}")
        return False
    except OSError as e:
        print(f"OS error: Ensure {CANADA_PARQUET} file exists")
        return False

if __name__ == "__main__":
    extract_phase()