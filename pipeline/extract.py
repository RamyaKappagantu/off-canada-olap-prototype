import duckdb

DB_PATH = 'off.duckdb'
CANADA_PARQUET = 'data/raw/off-canada.parquet'
RECIPES_CSV = 'data/raw/recipes_data.csv'
def extract_phase():
    try:
        with duckdb.connect(DB_PATH) as conn:
            print("Connected")
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_layer")
            print(f"Loading {CANADA_PARQUET} into raw_layer.off_products!!")
            # countries_tags has not been included since all of them are from "en:canada"
            conn.execute(f"""CREATE OR REPLACE TABLE raw_layer.off_products AS
                    SELECT brands, data_quality_errors_tags, food_groups_tags,
                    ingredients_original_tags, nutriments, nutriscore_score, product_name 
                    FROM read_parquet('{CANADA_PARQUET}')""")

            print(f"Loading {RECIPES_CSV} into raw_layer.recipes!!")
            conn.execute(f"""CREATE OR REPLACE TABLE raw_layer.recipes AS
                    SELECT * FROM read_csv_auto('{RECIPES_CSV}', header=True)""")
            # Other data sources can be added here

            #Verify whether the tables have been created
            data_count = conn.execute("SELECT COUNT(*) FROM raw_layer.off_products").fetchone()[0]
            recipes_count = conn.execute("SELECT COUNT(*) FROM raw_layer.recipes").fetchone()[0]
            print(f"Loaded {data_count} rows from the parquet file")
            print(f"Loaded {recipes_count} rows from the csv file")
            
        return True
    except duckdb.Error as e:
        print(f"Duckdb error during EL phase: {e}")
        return False
    except OSError as e:
        print(f"OS error: Ensure {CANADA_PARQUET} and {RECIPES_CSV} file exists")
        return False

if __name__ == "__main__":
    extract_phase()