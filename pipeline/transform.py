import duckdb

DB_PATH = "off.duckdb"
def transform_phase():
    try:
        with duckdb.connect(DB_PATH) as conn:
            print("Connected to off.duckdb!!")

            conn.execute("CREATE SCHEMA IF NOT EXISTS staging_layer")
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytical_layer")
            
            # create table for barcode in staging layer
            print("Creating table staging_layer.off_barcodes")
            transformation_query = """CREATE OR REPLACE TABLE staging_layer.off_barcodes AS
                                    WITH extracted_names AS (
                                    SELECT brands, code, data_quality_errors_tags,
                                    food_groups_tags,ingredients_original_tags, nutriments, 
                                    nutriscore_score, product_name, 
                                    unnest(list_filter(product_name, x -> x.lang == 'main'))['text'] main_product_name
                                    FROM raw_layer.off_products
                                    )
                                    SELECT
                                    brands,
                                    lower(trim(brands)) AS normalized_brand,
                                    code AS barcode,
                                    coalesce(lower(trim(brands)), 'unknown_brand') || '_' || lower(trim(main_product_name)) AS product_id,
                                    data_quality_errors_tags,
                                    CASE 
                                        WHEN data_quality_errors_tags IS NULL THEN FALSE
                                        ELSE TRUE 
                                    END AS has_quality_errors,
                                    coalesce(array_length(data_quality_errors_tags), 0) AS quality_error_count,
                                    food_groups_tags,
                                    ingredients_original_tags,
                                    nutriments,
                                    nutriscore_score AS nutriscore,
                                    product_name
                                    FROM extracted_names
                                    WHERE code is NOT NULL and main_product_name is not NULL
                                    """
            conn.execute(transformation_query)

            #Create logical products table for analytics
            print("Creating table analytical_layer.logical_products!!")
            logical_products_query = """CREATE OR REPLACE TABLE analytical_layer.logical_products AS
                                        SELECT row_number() over (order by product_id) AS logical_product_id,
                                        product_id,
                                        min(brands) AS canonical_brand,
                                        min(product_name) AS canonical_product_name,
                                        count(*) AS barcode_count
                                        FROM staging_layer.off_barcodes
                                        GROUP BY product_id
                                    """
            conn.execute(logical_products_query)

            #Create mapping between barcode and product
            print("Creating table analytical_layer.product_barcode_mapping!!")
            bridge_query = """CREATE OR REPLACE TABLE analytical_layer.product_barcode_mapping AS
                              SELECT b.barcode,
                              l.logical_product_id,
                              FROM staging_layer.off_barcodes b
                              JOIN analytical_layer.logical_products l
                              ON b.product_id = l.product_id
                            """
            conn.execute(bridge_query)

            #Verify whether the tables have been created
            barcode_count = conn.execute("""SELECT COUNT(*) FROM staging_layer.off_barcodes""").fetchone()[0]
            product_count = conn.execute("""SELECT COUNT(*) FROM analytical_layer.logical_products""").fetchone()[0]
            product_mapping_count = conn.execute("""SELECT COUNT(*) FROM analytical_layer.product_barcode_mapping""").fetchone()[0]

            duplicate_groups = conn.execute("""SELECT COUNT(*) FROM analytical_layer.logical_products WHERE barcode_count > 1""").fetchone()[0]

            print(f"Staging Layer barcodes table count: {barcode_count} rows")
            print(f"Analytical Layer products table count: {product_count} rows")
            print(f"Analytical Layer product mapping count: {product_mapping_count} rows")
            print(f"Duplicate bar codes count: {duplicate_groups} rows")
        return True
    
    except duckdb.Error as e:
        print(f"DuckDB error during Transform phase: {e}")
        return False

if __name__ == "__main__":
    transform_phase()
            



                                    
                                    