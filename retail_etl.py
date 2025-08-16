#!/usr/bin/env python
# coding: utf-8
"""
Retail ETL (mac-friendly)
- Downloads Kaggle dataset (ankitbansal06/retail-orders)
- Cleans with pandas (snake_case columns, typed numerics, parsed dates)
- Loads into PostgreSQL (default) or SQLite (fallback)
"""

import os
import zipfile
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
ZIP_PATH = DATA_DIR / "retail-orders.zip"
CSV_PATH = DATA_DIR / "orders.csv"
CLEAN_CSV = DATA_DIR / "orders_clean.csv"

def download():
    load_dotenv(override=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(
        "ankitbansal06/retail-orders",
        path=str(DATA_DIR),
        force=True
    )
    guessed = next(DATA_DIR.glob("retail-orders*.zip"))
    if guessed != ZIP_PATH:
        guessed.rename(ZIP_PATH)
    with zipfile.ZipFile(ZIP_PATH) as zf:
        members = [m for m in zf.namelist() if m.endswith("orders.csv")]
        if not members:
            raise FileNotFoundError("orders.csv not found in zip")
        zf.extract(members[0], str(DATA_DIR))
        extracted = DATA_DIR / members[0]
        if extracted != CSV_PATH:
            if CSV_PATH.exists():
                CSV_PATH.unlink()
            extracted.rename(CSV_PATH)
    print(f"[OK] Downloaded -> {CSV_PATH}")

def clean():
    df = pd.read_csv(CSV_PATH, na_values=["Not Available","unknown","N/A","na","NA"])

    # normalize headers to snake_case
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[^\w]+","_",regex=True)
    )

    # parse dates
    for col in ("order_date","ship_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # numeric coercions (present in this dataset)
    for c in ["sales","discount","profit","shipping_cost","quantity","postal_code"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # de-duplicate
    df = df.drop_duplicates()

    # convenience month column
    if "order_date" in df.columns:
        df["order_month"] = df["order_date"].dt.to_period("M").dt.to_timestamp()

    df.to_csv(CLEAN_CSV, index=False)
    print(f"[OK] Cleaned -> {CLEAN_CSV} ({len(df):,} rows)")
    return CLEAN_CSV

def get_engine():
    load_dotenv(override=True)
    backend = os.getenv("DB_BACKEND", "postgres").lower()
    if backend == "sqlite":
        uri = f"sqlite:///{ROOT/'retail.db'}"
    else:
        uri = os.getenv("PG_URI", "postgresql+psycopg2://postgres:postgres@localhost:5432/retail")
    return create_engine(uri)

def load_to_db(csv_path: Path):
    eng = get_engine()
    df = pd.read_csv(csv_path)
    df.to_sql(
        "df_orders",
        eng,
        index=False,
        if_exists="replace",
        method="multi",
        chunksize=50_000
    )
    print("[OK] Loaded table: df_orders")

if __name__ == "__main__":
    download()
    cleaned = clean()
    load_to_db(cleaned)
    print("[DONE]")
