import argparse
import glob
import pandas as pd
from pathlib import Path

class SplitParquet:

    def __init__(self, source, target, force_overwrite=False) -> None:

        if Path(source).is_file():
            self.pfiles=[Path(source)]
        else:
            self.pfiles=glob.glob(glob.escape(source) + "*.parquet")
        self.target=Path(target)

        self.force_overwrite=force_overwrite

        print(f"found {len(self.pfiles)} files in '{source}'")
        print(f"outputting to '{self.target}'")

    @staticmethod
    def bin_elo(elo):
        if elo<700:
            return '0-700'
        elif elo>=3000:
            return '3000-inf'
        else:
            lower=(elo//100)*100
            return f"{lower}-{lower+100}"

    @staticmethod
    def calc_game_type(time_control):
        min, inc=time_control.split('+', 1)
        sec=(int(min)*60)+(int(inc)*40)
        if sec < 29:
            return 'ultrabullet'
        if sec < 179:
            return 'bullet'
        if sec < 479:
            return 'blitz'
        if sec < 1499:
            return 'rapid'
        return 'classical'

    def get_parquet_path(self, elo_bin, year_month):
        # folder=self.target / Path(elo_bin) / Path(game_type)
        folder=self.target / Path(elo_bin)
        pfile=f"{year_month}.parquet"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / pfile

    def split(self):
        for pfile in self.pfiles:
            self.split_file(pfile)

    def split_file(self, pfile):
        print(f"splitting {pfile}")
        skipped=0
        matches=pd.read_parquet(pfile, engine='pyarrow')

        dfs={}
        for key, row in matches.iterrows():
            if row['White'] is None or row['Black'] is None:
                skipped+=1
                continue

            elo_bin=self.bin_elo(int(min(row['WhiteElo'], row['BlackElo'])))

            year_month=row['UTCDate'].strftime('%Y-%m')

            if not elo_bin in dfs:
                dfs[elo_bin]={}

            if not year_month in dfs[elo_bin]:
                dfs[elo_bin][year_month]=[]

            # dfs[elo_bin][game_type].append(row)
            dfs[elo_bin][year_month].append(row)

            if key>0 and key%100000==0:
                print(f"{key}")

        # for bin_elo, game_types in dfs.items():
        for bin_elo, year_months in dfs.items():
            for year_month, games in year_months.items():
                parquet_path=self.get_parquet_path(
                    elo_bin=bin_elo,
                    year_month=year_month)

                print(f"{bin_elo} {year_month} {parquet_path} ({len(games)})")

                if self.force_overwrite and Path(parquet_path).is_file():
                    Path.unlink(parquet_path)
                    print(f"deleted existing '{parquet_path}'")

                if not Path(parquet_path).is_file():
                    (pd.DataFrame(games).
                        astype({"UTCTime":str}).
                        to_parquet(
                            path=parquet_path, 
                            compression='gzip', 
                            engine='pyarrow',
                            index=False))
                else:
                    print(f"file exists '{parquet_path}'")

        print(f"skipped {skipped}")

if __name__=="__main__":
    

    parser=argparse.ArgumentParser()
    parser.add_argument('-i','--input-path', required=True)
    parser.add_argument('-o','--output-path', required=True)
    parser.add_argument('-f','--force-overwrite', action='store_true', default=False)
    args=parser.parse_args()
    
    spf=SplitParquet(source=args.input_path, 
                     target=args.output_path, 
                     force_overwrite=args.force_overwrite)
    spf.split()
