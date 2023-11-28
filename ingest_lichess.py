"""Download and process data from database.lichess.org"""
import argparse
from pathlib import Path
from ingester import ingest_lichess_data
import datetime


def main(start, end, pq_dir):
    """Download data with a check for existing parquet files."""
    # pq_dir = "lichess_parquet"
    pq_dir.mkdir(parents=True, exist_ok=True)

    years = range(start, end)
    months = range(1, 12)
    arguments = [(y, m + 1, pq_dir) for y in years for m in months]

    for a in arguments:
        if (Path(pq_dir) / f"{a[0]}_{a[1]:02}.parquet").exists():
            print(f"{a[0]}_{a[1]:02} exists. Skipping...")
            continue
        ingest_lichess_data(*a)


if __name__ == "__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=2013)
    parser.add_argument('--end', type=int, default=datetime.date.today().year)
    parser.add_argument('--parquet-dir', type=Path, default="./lichess_parquet")
    args=parser.parse_args()

    main(
        start=args.start, 
        end=args.end,
        pq_dir=args.parquet_dir)
