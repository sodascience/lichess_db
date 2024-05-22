"""Module to download and convert lichess data headers to parquet files directory."""
import io
from tempfile import NamedTemporaryFile as TempFile
import json
import re
import logging
import requests
from tqdm import tqdm
import zstandard as zstd
import polars as pl

def ingest_lichess_data(year: int, month: int, dir_parquet: str = "./lichess_parquet",
                        include_moves: bool = False):
    """
    Download, process, and convert chess games data from the Lichess database to Parquet format.

    This function streams a dataset from the Lichess database, extracts chess game data,
    and writes it to temporary NDJSON files. Once a certain number of games are processed,
    the data is converted to Polars dataframes and saved to Parquet files. This process repeats
    until all games in the dataset have been processed and saved.

    Parameters:
        year (int): The year of the dataset to download.
        month (str): The month of the dataset to download.
        dir_parquet (str, optional): Directory where Parquet files will be saved. Defaults
        to "../lichess_parquet".
        include_moves (bool, optional; default False): Whether to include games' moves in the
        saved data. Including moves greatly increases the size of the Parquet files.

    The function constructs a URL to stream the dataset from, uses regular expressions to parse
    the data, and utilizes Zstandard for decompression. Progress is tracked and displayed using
    a progress bar. To avoid memory issues when including moves, the number of games per file
    is limited to an arbitrary 1M games.

    The final dataset is written into the `dir_parquet` directory with filenames based on
    `year`, `month` and batch number.

    Example:
        ingest_lichess_data(2023, 1) # This will process games from January 2023.
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
        requests.get(url, stream=True, timeout=10) as response
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
        moves = None
        games = 0
        temp_files=[]

        # Start progres bar (approximate bytes since the raw file is
        # compressed and we are uncompressing on the fly)
        progress_bar = tqdm(
            total=num_bytes * 5.2,
            unit="iB",
            unit_scale=True,
            miniters=100,
            desc=f"{year}_{month:02}",
        )

        # Create temp file and store in list
        temp_files.append(TempFile(suffix=".ndjson", mode="w+"))

        logging.debug("Collecting data from stream")
        # Start loop
        for line in text_stream:
            progress_bar.update(len(line))
            if looking_for_game:
                 # Looking for the start of the game
                if line.startswith("["):
                    looking_for_game = False
            elif line.startswith("1."):
                moves = line
            elif not line.startswith("[") and moves is not None:
                # Game just ended, dump to NDJSON file
                if include_moves:
                    game.append(("Moves", moves))
                game_df=dict(game)
                # Add fields that are missing when they have no value
                for field in ['BlackTitle', 'WhiteTitle']:
                    if field not in game_df:
                        game_df.update({field: '-'})

                # Add concat DateTime field to replace seperate Date & TIme
                game_df.update({'DateTime': f"{game_df['UTCDate']} {game_df['UTCTime']}"})

                # Write complete game to temp file
                temp_files[-1].write(json.dumps(game_df) + "\n")

                looking_for_game = True
                game = []
                moves = None
                games += 1
                if games>=1e6:
                    # When 1M games reached, create new temp file
                    temp_files.append(TempFile(suffix=".ndjson", mode="w+"))
                    games = 0

            elif line.startswith("["):  # Game continues, keep appending
                game.append(re.findall(pattern, line)[0])

        # Avoid 'hanging' progress bars due to approximation/actual value-mismatch
        progress_bar.update(num_bytes * 5.2)
        # Clean up
        progress_bar.close()

    logging.debug("Converting ndjson to Parquet")
    # Convert temp files to parquet, one by one
    batch = 0
    for temp_file in temp_files:
        _ndjson_to_parquet(temp_file.name,
                           f"{dir_parquet}/{year}_{month:02}_{batch:003}.parquet", include_moves)
        batch += 1

def _ndjson_to_parquet(ndjson_path: str, parquet_path: str, include_moves: bool):
    """Creates a cleaned dataframe from an ndjson of Lichess game info."""
    cols = ["ID", "White", "Black", "Result", "WhiteElo", "BlackElo",
            "WhiteTitle", "BlackTitle", "WhiteRatingDiff", "BlackRatingDiff", "ECO",
            "Opening", "TimeControl", "Termination", "DateTime" ]

    schema = {
              "Site": pl.Utf8,
              "White": pl.Utf8, 
              "Black": pl.Utf8, 
              "Result": pl.Enum(["1-0", "0-1", "1/2-1/2", "?"]), 
              "WhiteElo": pl.Utf8, 
              "BlackElo": pl.Utf8, 
              "WhiteTitle": pl.Utf8, 
              "BlackTitle": pl.Utf8, 
              "WhiteRatingDiff": pl.Utf8, 
              "BlackRatingDiff": pl.Utf8, 
              "ECO": pl.Utf8, 
              "Opening": pl.Utf8, 
              "TimeControl": pl.Utf8, 
              "Termination": pl.Enum(["Time forfeit", "Rules infraction", "Normal", "?"]), 
              "DateTime": pl.Utf8
            }

    if include_moves:
        cols.append("Moves")
        schema["Moves"] = pl.Utf8

    int_cols = ["WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff"]

    logging.debug("Creating dataframe")
    lf = (
        # create lazy dataframe
        pl.scan_ndjson(ndjson_path, schema=schema)
        # transform all ? values into nulls
        # see here: https://stackoverflow.com/a/74816042
        .with_columns(pl.when(pl.all() != "?").then(pl.all()))
        # now, do light data transformation
        .with_columns(
            pl.col(int_cols).str.replace(r"\+", "").cast(pl.Int16),
            pl.col("DateTime").str.to_datetime(format="%Y.%m.%d %H:%M:%S"),
            pl.col("Site").str.replace("https://lichess.org/", "").alias("ID"),
        )
        # # lastly, select only what we need
        .select(cols)
    )

    logging.info("Writing '%s", parquet_path)
    
    # gzip and use_pyarrow are required for default Apache Drill compatibility
    lf.collect(streaming=True).write_parquet(parquet_path, compression='gzip', use_pyarrow=True)

    return parquet_path
