"""
Download and preprocess the Criteo 1M sample dataset for GBDT benchmarking.

Produces a single parquet file:
  criteo.parquet — I1-I13 as FLOAT, C1-C26 as STRING, label as INT

CatBoost uses all features (numeric + string categorical).
LightGBM only uses the numeric features (I1-I13) in its model definition.

Dataset schema (TSV, no header):
  label  I1..I13  C1..C26
  label: 0/1 click indicator
  I1-I13: 13 integer features (count-based, may have missing values)
  C1-C26: 26 categorical features (32-bit hashed strings, may be empty)
"""
import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = (
    ["label"]
    + [f"I{i}" for i in range(1, 14)]
    + [f"C{i}" for i in range(1, 27)]
)
NUMERIC_COLS = [f"I{i}" for i in range(1, 14)]
CATEGORICAL_COLS = [f"C{i}" for i in range(1, 27)]


def download_criteo_1m(output_dir):
    """Download the Criteo 1M dataset (~1M rows, ~240 MB) via kagglehub."""
    csv_path = os.path.join(output_dir, "criteo_1m.csv")
    if os.path.exists(csv_path):
        print(f"Criteo 1M already exists: {csv_path}")
        return csv_path

    import kagglehub
    dataset_path = kagglehub.dataset_download("datuman/criteo-ad-click-limited-1m")
    print(f"Downloaded via kagglehub to: {dataset_path}")
    # Find the data file (CSV or TSV) in the downloaded directory
    for f in os.listdir(dataset_path):
        if f.endswith((".csv", ".tsv")):
            src = os.path.join(dataset_path, f)
            shutil.move(src, csv_path)
            print(f"Moved {f} → {csv_path}")
            return csv_path
    raise FileNotFoundError(f"No CSV/TSV file found in {dataset_path}")


def load_criteo(file_path):
    """Load Criteo data, auto-detecting separator and header.

    The original Criteo dataset is TSV with no header, but Kaggle-hosted
    copies may use CSV format and/or include a header row.
    """
    with open(file_path, "r") as f:
        first_line = f.readline().strip()
    # Auto-detect separator: TSV if tabs present, otherwise CSV
    sep = "\t" if "\t" in first_line else ","
    # Auto-detect header: if first field is not 0/1, assume header row
    first_field = first_line.split(sep)[0]
    has_header = first_field not in ("0", "1")
    print(f"Detected separator={repr(sep)}, header={has_header}")
    # Always assign COLUMNS: when header=0, pandas skips the first row
    # then replaces names with COLUMNS, normalizing any header naming.
    df = pd.read_csv(
        file_path, sep=sep,
        header=0 if has_header else None,
        names=COLUMNS,
    )
    return df


def preprocess(df):
    """Preprocess: numeric → float32, categorical → string.

    CatBoost natively handles string categorical features.
    LightGBM only uses the numeric subset defined in its model definition.
    """
    out = df.copy()
    # Numeric: fill NaN with 0, cast to float32
    for col in NUMERIC_COLS:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("float32")
    # Categorical: fill empty with "-1", cast to string
    for col in CATEGORICAL_COLS:
        out[col] = out[col].astype(str).replace("nan", "-1")
    # Label: int32
    out["label"] = out["label"].astype("int32")
    return out


def main():
    output_dir = os.path.dirname(os.path.realpath(__file__))

    parquet_path = os.path.join(output_dir, "criteo.parquet")
    if os.path.exists(parquet_path):
        print(f"Parquet already exists: {parquet_path}, skipping.")
        return

    # Download
    csv_path = download_criteo_1m(output_dir)

    # Load and preprocess
    print(f"Loading {csv_path} ...")
    df = load_criteo(csv_path)
    print(f"Loaded {len(df)} rows")

    df = preprocess(df)

    # Save
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression="snappy")
    print(f"Saved: {parquet_path} ({len(df)} rows, {os.path.getsize(parquet_path) / 1e6:.1f} MB)")
    print("\n=== Criteo 1M dataset processing completed! ===")


if __name__ == "__main__":
    main()
