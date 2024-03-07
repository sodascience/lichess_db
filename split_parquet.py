"""Module to reorganize Lichess Parquet files."""
import argparse
import glob
import logging
from pathlib import Path
import pandas as pd
from tqdm import tqdm

def split_parquet(source: str, target: str) -> None:
    """Reorganize existing Parquet files with Lichess chess games into folders and files.

    Parameters:
        source (str): folder containing source Parquet files.
        target (str): root folder of resulting folders with Parquet files.

    The function will create folders based on bucketed Elo-ratings, containing batches of new
    Parquet files based on games' year and month of play. This restructuring is intended to
    support easier typical queries through Apache Drill.
    """
    if Path(source).is_file():
        pfiles=[Path(source)]
    else:
        pfiles=[Path(x) for x in glob.glob(str(Path(source).resolve()) + "/*.parquet")]

    logging.info("Found %s files in '%s'", len(pfiles), source)

    for pfile in pfiles:
        _split_file(target=Path(target), pfile=pfile)

def _bin_elo(elo: int) -> str:
    if elo<700:
        return '0-700'
    if elo>=3000:
        return '3000-inf'
    lower=(elo//100)*100
    return f"{lower}-{lower+100}"

def _get_parquet_path(target: Path, elo_bin: str, year_month: str) -> Path:

    def format_filename(batch):
        return f"{year_month}__{batch:003}.parquet"

    folder=target / Path(elo_bin)
    folder.mkdir(parents=True, exist_ok=True)
    batch = 0
    while Path(format_filename(batch)).is_file():
        batch += 1
    return folder / format_filename(batch)

def _split_file(pfile: Path, target: Path) -> None:

    # read source file
    matches = pd.read_parquet(pfile, engine='pyarrow')

    pbar=tqdm(desc=f"Reading '{pfile.name}'", total=len(matches))

    skipped = 0
    dfs = {}
    # iterate games
    for _, row in matches.iterrows():
        if row['White'] is None or row['Black'] is None:
            skipped += 1
            continue

        # determine Elo-bin
        elo_bin = _bin_elo(int(min(row['WhiteElo'], row['BlackElo'])))
        # get datestring
        year_month = row['UTCDate'].strftime('%Y-%m')

        if not elo_bin in dfs:
            dfs[elo_bin]={}

        if not year_month in dfs[elo_bin]:
            dfs[elo_bin][year_month]=[]

        # append game to appropriate bucket
        dfs[elo_bin][year_month].append(row)

        pbar.update(1)

    pbar.update(skipped)
    pbar.close()

    logging.info("Skipped %s games (black or white missing)", skipped)

    pbar=tqdm(desc="Writing", total=len(matches)-skipped)
    saved=[]

    # saving games
    for bin_elo, year_months in dfs.items():
        for year_month, games in year_months.items():
            parquet_path=_get_parquet_path(
                target=target,
                elo_bin=bin_elo,
                year_month=year_month)

            saved.append(f"{str(parquet_path)[len(str(target)):]} ({len(games)})")

            (pd.DataFrame(games)
                .astype({"UTCTime":str})
                .to_parquet(
                    path=parquet_path,
                    compression='gzip',
                    engine='pyarrow',
                    index=False))

            pbar.update(len(games))

    pbar.close()

    for item in saved:
        logging.debug(item)

if __name__=="__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument('-i','--input-path', required=True)
    parser.add_argument('-o','--output-path', required=True)
    parser.add_argument('--debug', action='store_true')
    args=parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    split_parquet(
        source=args.input_path,
        target=args.output_path)
