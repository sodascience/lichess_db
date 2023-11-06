from pathlib import Path
import polars as pl
from plotnine import (
    ggplot,
    geom_line,
    geom_col,
    aes,
    theme,
    element_text,
    coord_flip,
    theme_538,
    labs,
)
from data_loader import LichessDataLoader
from lichess_download import LichessDownloader, decompress_file

# Folder paths
raw_folder = Path("raw_data")
pgn_folder = Path("decompressed_data")
pqt_folder = Path("lichess_parquet")

# download files
downloader = LichessDownloader(data_folder=raw_folder)
for year in [2013]:
    for month in ["01"]:
        data_url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month}.pgn.zst"
        downloader.download_file(url=data_url)

# decompress zst files
for zstfile in raw_folder.glob("*.zst"):
    decompress_file(input_file=zstfile, output_dir=pgn_folder)

# convert pgn files to parquet
converter = LichessDataLoader(parquet_dir=pqt_folder)
for pgn_file in pgn_folder.glob("*.pgn"):
    converter.convert_pgn(pgn_path=pgn_file)

# do some fun stuff!
df = pl.read_parquet("lichess_parquet", use_pyarrow=True).lazy()

gamecount = df.group_by("UTCDate").agg(pl.count()).sort("UTCDate").collect()

openingcount = (
    df.group_by(["Opening"])
    .agg(pl.count())
    .sort("count", descending=True)
    .limit(20)
    .with_columns(pl.col("Opening").cast(pl.Categorical))
    .collect()
)

gamecount_plot = (
    ggplot(gamecount, aes(x="UTCDate", y="count"))
    + geom_line()
    + theme_538()
    + theme(axis_text_x=element_text(rotation=45))
    + labs(x="", y="Number of chess games played", title="Lichess popularity")
)

gamecount_plot.save("img/gamecount_plot.png", width=10, height=6)

opening_plot = (
    ggplot(openingcount, aes(x="Opening", y="count"))
    + geom_col()
    + theme(axis_text_x=element_text(rotation=90))
    + coord_flip()
    + theme_538()
    + labs(x="Opening", y="Number of uses", title="20 most common openings in 2013")
)

opening_plot.save("img/opening_plot.png", width=8, height=8)
