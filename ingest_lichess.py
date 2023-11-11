"""Download and process data from database.lichess.org"""
from pathlib import Path
from ingester import ingest_lichess_data


def main():
    """Download data with a check for existing parquet files."""
    pq_dir = "lichess_parquet"
    years = range(2013, 2024)
    months = range(1, 13)
    arguments = [(y, m + 1, pq_dir) for y in years for m in months]

    for a in arguments:
        if (Path(pq_dir) / f"{a[0]}_{a[1]:02}.parquet").exists():
            print(f"{a[0]}_{a[1]:02} exists. Skipping...")
            continue
        ingest_lichess_data(*a)


if __name__ == "__main__":
    main()
