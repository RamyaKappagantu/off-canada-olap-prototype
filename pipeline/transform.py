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
            off_barcodes_query = """CREATE OR REPLACE TABLE staging_layer.off_barcodes AS
                                    WITH extracted_names AS (
                                    SELECT brands, code, data_quality_errors_tags,
                                    food_groups_tags,ingredients_original_tags, nutriments, 
                                    nutriscore_score, product_name, 
                                    unnest(list_filter(product_name, x -> x.lang == 'main'))['text'] main_product_name
                                    FROM raw_layer.off_products
                                    )
                                    SELECT
                                    row_number() OVER() AS off_record_id,
                                    brands,
                                    lower(trim(brands)) AS normalized_brand,
                                    code AS barcode,
                                    lower(trim(main_product_name)) AS normalized_product_name,
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
                                    product_name,
                                    CASE 
                                        WHEN brands is NOT NULL and trim(brands) != '' and product_name is NOT NULL and product_name != '[]' THEN TRUE
                                        ELSE FALSE
                                        END AS is_groupable,
                                    CASE 
                                        WHEN is_groupable = TRUE THEN lower(trim(brands)) || '_' || lower(trim(main_product_name))
                                        ELSE NULL
                                    END AS logical_product_key
                                    FROM extracted_names
                                    WHERE code is NOT NULL and product_name is not NULL
                                    """
            conn.execute(off_barcodes_query)

            #Create logical products table for analytics
            print("Creating table analytical_layer.logical_products!!")
            logical_products_query = """CREATE OR REPLACE TABLE analytical_layer.logical_products AS
                                        SELECT row_number() over (order by logical_product_key) AS logical_product_id,
                                        logical_product_key,
                                        min(brands) AS canonical_brand,
                                        min(normalized_product_name) AS canonical_product_name,
                                        count(*) AS barcode_count
                                        FROM staging_layer.off_barcodes
                                        WHERE is_groupable = TRUE AND logical_product_key IS NOT NULL
                                        GROUP BY logical_product_key
                                    """
            conn.execute(logical_products_query)

            #Create mapping between barcode and product
            print("Creating table analytical_layer.product_barcode_mapping!!")
            mapping_query = """CREATE OR REPLACE TABLE analytical_layer.product_barcode_mapping AS
                              SELECT b.off_record_id,
                              b.barcode,
                              l.logical_product_id
                              FROM staging_layer.off_barcodes b
                              JOIN analytical_layer.logical_products l
                              ON b.logical_product_key = l.logical_product_key
                              WHERE b.is_groupable = TRUE
                            """
            conn.execute(mapping_query)

            #Create table for recipe in staging layer
            print("Creating table staging_layer.recipe_ingredients")
            recipe_ingredients_query = """ CREATE OR REPLACE TABLE staging_layer.recipe_ingredients AS
                                            WITH cleaned_table AS (
                                            SELECT id AS recipe_id, name AS recipe_name, url AS recipe_url,
                                            replace(replace(ingredients, '[', ''), ']', '') AS ingredients_cleaned
                                            FROM raw_layer.recipes
                                            ),
                                            split_ingredients AS (
                                            SELECT recipe_id, recipe_name, recipe_url,
                                            unnest(string_split(ingredients_cleaned, ',')) AS ingredient_raw
                                            FROM cleaned_table
                                            )
                                            SELECT recipe_id, recipe_name, recipe_url,
                                            trim(replace(ingredient_raw, '"', '')) AS ingredient_tag,
                                            replace(trim(replace(ingredient_raw, '"', '')),'en:', '' ) AS normalized_ingredient
                                            FROM split_ingredients
                                            WHERE trim(replace(ingredient_raw, '"', '')) != ''
                                            """
            conn.execute(recipe_ingredients_query)

            #Create table to map ingredients between products table and recipes table
            print("Creating table staging_layer.product_ingredient_mapping")
            product_ingredient_query = """CREATE OR REPLACE TABLE staging_layer.product_ingredient_mapping AS
                                            WITH cleaned_table AS (
                                            SELECT off_record_id, barcode, 
                                            unnest(ingredients_original_tags) AS ingredients_raw
                                            FROM staging_layer.off_barcodes where ingredients_original_tags is NOT NULL 
                                            AND ingredients_original_tags != '[]'
                                            )
                                            SELECT off_record_id, barcode,
                                            replace(ingredients_raw, 'en:', '') AS normalized_ingredient
                                            FROM cleaned_table
                                            """
            conn.execute(product_ingredient_query)

            #Create table in the analytical layer to check if there is a match between recipe ingredient and product ingredient tags
            print("Creating table analytical_layer.ingredient_tag_match")
            ingredient_tag_query = """CREATE OR REPLACE TABLE analytical_layer.ingredient_tag_match AS
                                        SELECT r.recipe_id, r.recipe_name, r.recipe_url, r.normalized_ingredient,
                                        p.off_record_id, p.barcode, p.normalized_ingredient
                                        FROM staging_layer.recipe_ingredients r 
                                        JOIN staging_layer.product_ingredient_mapping p
                                        ON r.normalized_ingredient = p.normalized_ingredient"""
            conn.execute(ingredient_tag_query)


            #Verify whether the tables have been created
            barcode_count = conn.execute("""SELECT COUNT(*) FROM staging_layer.off_barcodes""").fetchone()[0]
            product_count = conn.execute("""SELECT COUNT(*) FROM analytical_layer.logical_products""").fetchone()[0]
            product_mapping_count = conn.execute("""SELECT COUNT(*) FROM analytical_layer.product_barcode_mapping""").fetchone()[0]
            recipe_ingredients_count = conn.execute("""SELECT COUNT(*) FROM staging_layer.recipe_ingredients""").fetchone()[0]
            product_ingredient_mapping_count = conn.execute("""SELECT COUNT(*) FROM staging_layer.product_ingredient_mapping""").fetchone()[0]
            ingredient_tag_match_count = conn.execute("""SELECT COUNT(*) FROM analytical_layer.ingredient_tag_match""").fetchone()[0]

            duplicate_groups = conn.execute("""SELECT COUNT(*) FROM analytical_layer.logical_products WHERE barcode_count > 1""").fetchone()[0]

            print(f"Staging Layer barcodes table count: {barcode_count} rows")
            print(f"Staging Layer recipe ingredients count: {recipe_ingredients_count} rows")
            print(f"Staging Layer product ingredient mapping count: {product_ingredient_mapping_count} rows")
            print(f"Analytical Layer products table count: {product_count} rows")
            print(f"Analytical Layer product mapping count: {product_mapping_count} rows")
            print(f"Analytical Layer ingredient tag match count: {ingredient_tag_match_count} rows")
            print(f"Duplicate bar codes count: {duplicate_groups} rows")
        return True
    
    except duckdb.Error as e:
        print(f"DuckDB error during Transform phase: {e}")
        return False

if __name__ == "__main__":
    transform_phase()
            



                                    
                                    