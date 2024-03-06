"""Module to split Lichess Parquet files."""
import argparse
import glob
import logging
import pandas as pd
from pathlib import Path

logger=logging.getLogger()

def split_parquet(source: str, target: str, mode: str='append') -> None:
    """Split existing Parquet files with Lichess chess games into folders and files.

    Parameters:
        source (str): folder containing source Parquet files.
        target (str): root folder of resulting folders with Parquet files.
        mode {append, overwrite}: append to existing Parquet files or overwrite them.

    The function will create folders based on Elo-rating buckets, and new Parquet files based
    on games' year and month of play. This restructuring is intended to allow easier typical 
    queries (typically through Apache Drill).
    """
    if Path(source).is_file():
        pfiles=[Path(source)]
    else:
        pfiles=glob.glob(str(Path(source).resolve()) + "/*.parquet")

    logger.info(f"Found {len(pfiles)} files in '{source}'")

    for pfile in pfiles:
        _split_file(target=Path(target), pfile=pfile, mode=mode)

def _bin_elo(elo):
    if elo<700:
        return '0-700'
    if elo>=3000:
        return '3000-inf'
    lower=(elo//100)*100
    return f"{lower}-{lower+100}"

def _get_parquet_path(target, elo_bin, year_month):
    folder=target / Path(elo_bin)
    pfile=f"{year_month}.parquet"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / pfile

def _split_file(pfile, target, mode):
    logger.info(f"Splitting '{pfile}'")

    # read source file
    matches = pd.read_parquet(pfile, engine='pyarrow')

    skipped = 0
    dfs = {}
    # iterate games
    for key, row in matches.iterrows():
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

        if key>0 and key%100000==0:
            logger.debug(f"{key}")

    # saving games
    for bin_elo, year_months in dfs.items():
        for year_month, games in year_months.items():
            parquet_path=_get_parquet_path(
                target=target,
                elo_bin=bin_elo,
                year_month=year_month)

            logger.info(f"{bin_elo} {year_month} --> {parquet_path} ({len(games)})")

            new_df = pd.DataFrame(games).astype({"UTCTime":str})

            if Path(parquet_path).is_file():
                if mode=='append':
                    # parquet has no append, so read existing data, concat to new, and delete file
                    new_df = pd.concat([pd.read_parquet(parquet_path, engine='pyarrow'), new_df])
                Path.unlink(parquet_path)
            
            new_df.astype({"UTCTime":str}).to_parquet(
                path=parquet_path,
                compression='gzip',
                engine='pyarrow',
                index=False)

    logger.info(f"Skipped {skipped} games (black or white missing)")

if __name__=="__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument('-i','--input-path', required=True)
    parser.add_argument('-o','--output-path', required=True)
    parser.add_argument('--mode', choices=['overwrite', 'append'], default='append')
    parser.add_argument('--debug', action='store_true')
    args=parser.parse_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    split_parquet(
        source=args.input_path,
        target=args.output_path,
        mode=args.mode)
