"""Download and process data from database.lichess.org"""
import logging
import argparse
import datetime
from pathlib import Path
from ingester import ingest_lichess_data


def main(start, end, pq_dir, months=None, include_moves=False):
    """Download data with a check for existing parquet files."""
    # pq_dir = "lichess_parquet"
    pq_dir.mkdir(parents=True, exist_ok=True)

    years = range(start, end)
    if months is None:
        months = range(1, 13)
    arguments = [(y, m, pq_dir, include_moves) for y in years for m in months]

    for arg in arguments:
        if (Path(pq_dir) / f"{arg[0]}_{arg[1]:02}.parquet").exists():
            print(f"{arg[0]}_{arg[1]:02} exists. Skipping...")
            continue
        ingest_lichess_data(*arg)


if __name__ == "__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=2013)
    parser.add_argument('--end', type=int, default=datetime.date.today().year)
    parser.add_argument('--months', nargs='+', type=int)
    parser.add_argument('--include-moves', action='store_true', default=False)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--parquet-dir', type=Path, default="./lichess_parquet")
    args=parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    main(
        start=args.start,
        end=args.end,
        months=args.months,
        include_moves=args.include_moves,
        pq_dir=args.parquet_dir)
