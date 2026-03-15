# Project Overview
This project explores the feasibility of building a Canadian OLAP database for Open Food Facts (OFF) using DuckDB. The goal is to investigate how OFF product data can be transformed into an analytical data model suitable for downstream applications such as recipe extensions, online store extension, and AI-powered product discovery.

The prototype implements a small ELT pipeline that:

- Extracts Canadian product data from the OFF Parquet dataset.

- Loads the raw data into DuckDB.

- Transforms the barcode-level OFF records into a structured staging layer.

- Builds a logical product layer based on normalized Brand + ProductName.

- Runs analytical queries to evaluate the impact of this design decision.

This prototype specifically investigates whether Brand + ProductName can serve as a logical product identifier, linking multiple barcodes to a single analytical product entity while still preserving the original barcode-level records.

The results from this analysis help assess whether such a design is viable for the Canadian OLAP database.
# Repository structure
off-canada-olap-prototype/
│
├── data/
│   └── raw/
│       ├── off-canada.parquet
│       └── recipes_data.csv
│
├── pipeline/
│   ├── extract.py
│   ├── transform.py
│   └── analysis.py
│
├── .gitignore
|    ├── off.duckdb
│
└── README.md
# How to run

