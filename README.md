# Project Overview
This project explores the feasibility of building a Canadian OLAP database for Open Food Facts (OFF) using DuckDB. The goal is to investigate how OFF product data can be transformed into an analytical data model suitable for downstream applications such as recipe extensions, online store extension, and AI-powered product discovery.

The prototype implements a small ELT pipeline that:

- Extracts Canadian product data from the OFF Parquet dataset.

- Loads the raw data and recipes data from Canada Food Guide into DuckDB.

- Transforms the barcode-level OFF records into a structured staging layer.

- Builds a logical product layer based on normalized Brand + ProductName.

- Runs analytical queries to evaluate the impact of this design decision.

This prototype specifically investigates whether Brand + ProductName can serve as a logical product identifier, linking multiple barcodes to a single analytical product entity while still preserving the original barcode-level records.

The results from this analysis help assess whether such a design is viable for the Canadian OLAP database.
# Key Files
- extract.py - This is a python file which contains the logic to ingest the input sources i.e OFF Canada Parquet and Recipies dataset from Canada Food Guide
- transform.py - This is a python file which creates tables in the staging layer and analytical layer. These transformations normalize product data and construct logical product entities.
- analysis.py - This is a python file which performs analytical queries and provides key metrics for initial investigation like number of barcode records, number of logical level products, number of duplicate groups, product groups with distinct nutriscore values
# How to run
- Install dependencies: duckdb, pandas
```python
pip3 install duckdb pandas
- Place the datasets in data/raw folder
- Run extract.py - The output should create tables in the raw layer
```python
python3 pipeline\extract.py
- Run transform.py - The output should create tables in the staging and analytical layer and successfully perform transformations
```python
python3 pipeline\transform.py
- Run analysis.py - The output should display the required metrics
```python
python3 pipeline\analysis.py
