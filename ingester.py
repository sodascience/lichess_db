"""Module to download and convert lichess data headers to parquet files directory."""
import io
import tempfile
import json
import re
import requests
from tqdm import tqdm
import zstandard as zstd
import polars as pl

def ingest_lichess_data(year: int, month: int, dir_parquet: str = "lichess_parquet"):
    """
    Download, process, and convert chess games data from the Lichess database to Parquet format.

    This function streams a dataset from the Lichess database, extracts chess game data,
    and writes it to a temporary NDJSON file. Once a certain number of games are processed,
    the data is converted to a Polars dataframe and saved to a Parquet file. This process repeats
    until all games in the dataset have been processed and saved.

    Parameters:
        year (int): The year of the dataset to download.
        month (str): The month of the dataset to download.
        dir_parquet (str, optional): Directory where Parquet files will be saved. Defaults to "../lichess_parquet".

    The function constructs a URL to stream the dataset from, uses regular expressions to parse the data,
    and utilizes Zstandard for decompression. Progress is tracked and displayed using a progress bar.

    The final dataset is written into the `dir_parquet` directory with a filename based on the `year` and `month`

    Example:
        process_lichess_data(2023, 1) # This will process games from January 2023.
    """

    # Create data URL
    file_name = f"lichess_db_standard_rated_{year}-{month:02}.pgn.zst"
    url = f"https://database.lichess.org/standard/{file_name}"

    # Regex pattern to split lines with format [key\s"info"]
    pattern = re.compile(r'\[(.*?)\s"(.*)"\]')

    # Set up the decompressor
    decompressor = zstd.ZstdDecompressor(max_window_size=2**31)

    # Connect to url and create tempfile
    with (
        requests.get(url, stream=True, timeout=1) as response,
        tempfile.TemporaryFile(suffix=".ndjson", mode="w+") as temp_file,
    ):
        # get basic info, make sure connection was successful
        response.raise_for_status()
        num_bytes = int(response.headers.get("content-length", 0))

        # Decompress the response on the fly and read it line by line
        reader = decompressor.stream_reader(response.raw)
        text_stream = io.TextIOWrapper(reader, encoding="utf-8")

        # set up required vars for looping over each line
        looking_for_game = True
        game = []

        # Start progres bar (approximate bytes since the raw file is
        # compressed and we are uncompressing on the fly)
        progress_bar = tqdm(
            total=num_bytes * 5.2,
            unit="iB",
            unit_scale=True,
            miniters=100,
            desc=f"{year}_{month:02}",
        )

        # Start loop
        for line in text_stream:
            progress_bar.update(len(line))
            if looking_for_game:  # Looking for the start of the game
                if line.startswith("["):
                    looking_for_game = False
            elif not line.startswith("["):  # Game just ended, dump to NDJSON file
                looking_for_game = True
                temp_file.write(json.dumps(dict(game)) + "\n")
                game = []
            else:  # Game continues, keep appending
                game.append(re.findall(pattern, line)[0])

        # Clean up
        progress_bar.close()

        # convert to parquet
        _ndjson_to_parquet(temp_file.name, f"{dir_parquet}/{year}_{month:02}.parquet")


def _ndjson_to_parquet(ndjson_path: str, parquet_path: str):
    """Creates a cleaned dataframe from an ndjson of lichess header info."""
    int_cols = ["WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff"]
    df = (
        # create lazy dataframe
        pl.scan_ndjson(ndjson_path)
        # transform all ? values into nulls
        # see here: https://stackoverflow.com/a/74816042
        .with_columns(pl.when(pl.all() != "?").then(pl.all()))
        # now, do light data transformation
        .with_columns(
            pl.col(int_cols).cast(pl.Int32),
            pl.col("UTCDate").str.to_date(format="%Y.%m.%d"),
            pl.col("UTCTime").str.to_time(),
            pl.col("Site").str.replace("https://lichess.org/", "").alias("ID"),
        )
        # lastly, select only what we need
        .select(
            [
                "ID",
                "UTCDate",
                "UTCTime",
                "White",
                "Black",
                "Result",
                "WhiteElo",
                "BlackElo",
                "WhiteRatingDiff",
                "BlackRatingDiff",
                "ECO",
                "Opening",
                "TimeControl",
                "Termination",
            ]
        )
    )
    df.collect(streaming=True).write_parquet(parquet_path)
    return parquet_path
