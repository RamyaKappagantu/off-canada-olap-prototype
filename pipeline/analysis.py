import duckdb
import pandas as pd

DB_PATH = 'off.duckdb'

def run_query(conn, query, label):
    print(f"------ {label} ------")
    result = conn.execute(query).fetchdf()
    print(result.to_string(index=False))

def analysis_phase():
    try:
        with duckdb.connect(DB_PATH) as conn:
            print(f"Connected to {DB_PATH}!!")
            barcode_level_count_query = """SELECT COUNT(*) AS barcode_rows FROM
                                            staging_layer.off_barcodes"""
            run_query(conn, barcode_level_count_query, "Barcode-level rows count")

            duplicate_product_query = """SELECT COUNT(*) AS duplicate_groups FROM
                                            analytical_layer.logical_products
                                            WHERE barcode_count > 1"""
            run_query(conn, duplicate_product_query, "Duplicate logical Product groups")

            missing_brands_query = """SELECT COUNT(*) AS missing_brands_rows,
                                        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging_layer.off_barcodes), 2) AS missing_percentage
                                        FROM staging_layer.off_barcodes
                                        WHERE brands is NULL or trim(brands) = ''"""
            run_query(conn, missing_brands_query, "Products with missing brand name")

            quality_errors_count_query = """SELECT COUNT(*) AS rows_with_quality_errors,
                                            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging_layer.off_barcodes), 2) AS percentage_quality_errors
                                            FROM staging_layer.off_barcodes
                                            WHERE has_quality_errors = TRUE"""
            run_query(conn, quality_errors_count_query, "Products with quality errors")

            empty_product_name_query = """SELECT COUNT(*) AS empty_product_name_rows,
                                                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM staging_layer.off_barcodes), 2) AS empty_product_naem_percent
                                                FROM staging_layer.off_barcodes WHERE product_name = '[]'"""
            run_query(conn, empty_product_name_query, "Rows with Empty Product names")

            largest_duplicate_groups_query = """SELECT canonical_brand, canonical_product_name, barcode_count
                                                FROM analytical_layer.logical_products
                                                WHERE barcode_count > 1
                                                ORDER BY barcode_count DESC
                                                LIMIT 20"""
            run_query(conn, largest_duplicate_groups_query, "Top duplicate groups")

            nutriscore_inconsistency_query = """SELECT logical_product_key, COUNT(*) AS barcode_count,
                                                COUNT(distinct nutriscore) AS nutriscore_variants
                                                FROM staging_layer.off_barcodes
                                                WHERE is_groupable = TRUE AND logical_product_key is NOT NULL
                                                GROUP BY logical_product_key
                                                HAVING COUNT(*) > 1
                                                ORDER BY nutriscore_variants DESC, barcode_count DESC
                                                LIMIT 20"""
            run_query(conn, nutriscore_inconsistency_query, "Groups with multiple nutriscore values")

            common_off_ingredient_query = """SELECT normalized_ingredient, COUNT(*) AS frequency
                                                FROM staging_layer.product_ingredient_mapping
                                                GROUP BY normalized_ingredient
                                                ORDER BY frequency DESC LIMIT 20"""
            run_query(conn, common_off_ingredient_query, "Top 20 most common OFF ingredient")

            no_ingredient_recipe_match_query = """SELECT r.normalized_ingredient
                                                    FROM staging_layer.recipe_ingredients r
                                                    LEFT JOIN analytical_layer.ingredient_tag_match m
                                                    ON r.normalized_ingredient = m.normalized_ingredient
                                                    WHERE m.barcode is NULL GROUP BY r.normalized_ingredient
                                                    ORDER BY r.normalized_ingredient"""
            run_query(conn, no_ingredient_recipe_match_query, "Recipe Ingredients with no OFF ingredient match")
        return True

    
    except duckdb.Error as e:
        print(f"DuckDB error during analysis phase: {e}")
        return False
    
if __name__ == "__main__":
    analysis_phase()