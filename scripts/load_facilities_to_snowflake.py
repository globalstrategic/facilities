#!/usr/bin/env python3
"""
Load facilities parquet into MIKHAIL.ENTITY.FACILITY table.

Usage:
    python scripts/load_facilities_to_snowflake.py [parquet_file]

If no parquet_file is specified, uses the latest in output/entityidentity_export/
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def get_snowflake_connection():
    """Get Snowflake connection using 'tal' connection config."""
    # Read private key
    key_path = Path.home() / ".snowsql" / "rsa_key.p8"
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account="HDHACZZ-AE73585",
        user="WILLIAM.BRODHEAD",
        private_key=pkb,
        warehouse="GSMC_WH_XS",
        database="MIKHAIL",
        schema="ENTITY",
        role="ACCOUNTADMIN"
    )


def get_latest_parquet() -> Path:
    """Get the most recent parquet file from export directory."""
    export_dir = Path(__file__).parent.parent / "output" / "entityidentity_export"
    parquet_files = list(export_dir.glob("facilities_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {export_dir}")
    return max(parquet_files, key=lambda p: p.stat().st_mtime)


def load_facilities(parquet_path: Path, dry_run: bool = False):
    """Load facilities from parquet into Snowflake."""

    print(f"Loading facilities from: {parquet_path}")

    # Read parquet
    df = pd.read_parquet(parquet_path)
    print(f"  Total facilities in parquet: {len(df)}")

    # Map parquet columns to Snowflake columns
    # Snowflake table: MIKHAIL.ENTITY.FACILITY
    snowflake_df = pd.DataFrame({
        'FACILITY_KEY': df['facility_id'],
        'FACILITY_ID': df['facility_id'],
        'NAME': df['facility_name'],
        'NAME_NORM': df['facility_name'].str.lower().str.replace(r'[^a-z0-9]', '', regex=True),
        'FACILITY_NAME': df['facility_name'],
        'FACILITY_TYPE': df['facility_type'],
        'ADMIN1': df['admin1'],
        'LATITUDE': df['lat'],
        'LONGITUDE': df['lon'],
        'OPERATING_STATUS': df['operating_status'],
        'PRECISION_LEVEL': df['geo_precision'],
        'PRIMARY_METAL': df['primary_metal'],
        'SECONDARY_METAL': df['secondary_metal'],
        'OTHER_METALS': df['other_metals'],
        'COMPANY_NAMES': df['company_names'],
        'NOTES': df['verification_notes'],
    })

    # We need to look up COUNTRY_ID from ENTITY.COUNTRY
    # For now, store country_iso3 and join later
    country_iso3 = df['country_iso3']

    print(f"  Facilities with company_names: {snowflake_df['COMPANY_NAMES'].notna().sum()}")
    print(f"  Facilities with secondary_metal: {snowflake_df['SECONDARY_METAL'].notna().sum()}")
    print(f"  Facilities with primary_metal: {snowflake_df['PRIMARY_METAL'].notna().sum()}")

    if dry_run:
        print("\n[DRY RUN] Would load the following:")
        print(snowflake_df.head(10).to_string())
        return

    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    conn = get_snowflake_connection()

    try:
        cur = conn.cursor()

        # Get country ID mapping
        print("  Loading country mappings...")
        cur.execute("SELECT ID, ISO3 FROM ENTITY.COUNTRY")
        country_map = {row[1]: row[0] for row in cur.fetchall()}

        # Add COUNTRY_ID column
        snowflake_df['COUNTRY_ID'] = country_iso3.map(country_map)

        # Count missing countries
        missing_country = snowflake_df['COUNTRY_ID'].isna().sum()
        if missing_country > 0:
            missing_codes = country_iso3[snowflake_df['COUNTRY_ID'].isna()].unique()
            print(f"  Warning: {missing_country} facilities have unmapped countries: {list(missing_codes)[:10]}")

        # Truncate existing data
        print("  Truncating ENTITY.FACILITY...")
        cur.execute("TRUNCATE TABLE ENTITY.FACILITY")

        # Insert new data
        print(f"  Inserting {len(snowflake_df)} facilities...")

        # Use write_pandas for efficient bulk insert
        success, nchunks, nrows, _ = write_pandas(
            conn,
            snowflake_df,
            'FACILITY',
            database='MIKHAIL',
            schema='ENTITY',
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False
        )

        print(f"  Inserted {nrows} rows in {nchunks} chunks")

        # Verify
        cur.execute("SELECT COUNT(*) FROM ENTITY.FACILITY")
        count = cur.fetchone()[0]
        print(f"\n[OK] ENTITY.FACILITY now has {count} rows")

        # Show sample with new columns
        cur.execute("""
            SELECT FACILITY_KEY, NAME, PRIMARY_METAL, SECONDARY_METAL, COMPANY_NAMES
            FROM ENTITY.FACILITY
            WHERE SECONDARY_METAL IS NOT NULL
            LIMIT 5
        """)
        print("\nSample facilities with secondary metal:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} | {row[2]} | {row[3]} | {row[4][:50] if row[4] else None}...")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load facilities into Snowflake")
    parser.add_argument("parquet_file", nargs="?", help="Path to parquet file (default: latest)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without loading")
    args = parser.parse_args()

    if args.parquet_file:
        parquet_path = Path(args.parquet_file)
    else:
        parquet_path = get_latest_parquet()

    if not parquet_path.exists():
        print(f"Error: File not found: {parquet_path}", file=sys.stderr)
        sys.exit(1)

    load_facilities(parquet_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
