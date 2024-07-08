"""Module to download and convert lichess data headers to parquet files directory."""
import io
import logging
import json
import re
import requests

import zstandard as zstd
import polars as pl
import s3fs

from collections import defaultdict
from random import random
from tempfile import NamedTemporaryFile as TempFile
from tqdm import tqdm
from typing import Optional

def ingest_lichess_data(year: int,
                        month: int,
                        dir_parquet: str = "./lichess_parquet",
                        include_moves: bool = False,
                        fs: Optional[s3fs.core.S3FileSystem] = None,
                        dir_ndjson: Optional[str] = None,
                        ndjson_size: int = 1e6):
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
        fs (s3fs.core.S3FileSystem, optional): If provided, the function will use this filesystem
        to read and write files. Defaults to None.
        dir_ndjson (str, optional): Directory where NDJSON files will be saved. Defaults to None.
        ndjson_size (int, optional): The number of games to process before converting to Parquet.

    The function constructs a URL to stream the dataset from, uses regular expressions to parse
    the data, and utilizes Zstandard for decompression. Progress is tracked and displayed using
    a progress bar. To avoid memory issues when including moves, the number of games per file
    is limited to an arbitrary 1M games.

    The final dataset is written into the `dir_parquet` directory with filenames based on
    `year`, `month` and batch number.

    Example:
        ingest_lichess_data(2023, 1) # This will process games from January 2023.
    """

    # Use temp file for cumulative values
    try:
        if isinstance(fs, s3fs.core.S3FileSystem):
            # read cum_files from S3 point
            with fs.open(f"{dir_parquet}/cum_files.json.zst", mode='rb') as fin:
                decompressed_bytes = zstd.ZstdDecompressor().decompress(fin.read())
        else:
            # Read and decompress the data
            with open(f"{dir_parquet}/cum_files.json.zst", 'rb') as fin:
                decompressed_bytes = zstd.ZstdDecompressor().decompress(fin.read())

        # Convert bytes back to JSON object
        d_cum_games = json.loads(decompressed_bytes.decode('utf-8'))


    except FileNotFoundError:
        logging.debug("Cumulative file not found, recreating from scratch")
        d_cum_games = dict()
        d_cum_games["All"] = defaultdict(int)
        
    
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
        batch = 0
        if dir_ndjson is None:
            temp_file = TempFile(suffix=".ndjson", mode="w+")
        else:
            # useful for debugging
            temp_file = open(f"{dir_ndjson}/temp.ndjson", mode="w+")

        # Start progres bar (approximate bytes since the raw file is
        # compressed and we are uncompressing on the fly)
        progress_bar = tqdm(
            total=num_bytes * 5.2,
            unit="iB",
            unit_scale=True,
            miniters=100,
            desc=f"{year}_{month:02}",
        )

        logging.debug("Collecting data from stream")
        # Start loop
        for line in text_stream:
            progress_bar.update(len(line))
            if looking_for_game:
                 # Looking for the start of the game
                if line.startswith("["):
                    looking_for_game = False
                    # Add type of game 
                    event_type = list(re.findall(pattern, line)[0])
                    tournament = "tournament" in line
                    if tournament:
                        event_type[1] = event_type[1].split("tournament")[0].strip()
                    game.append(tuple(event_type))
                    

            elif line.startswith("1."):
                if include_moves:
                    # Keep only 3 moves
                    moves = line.replace("\n", " ").strip()
                    moves = moves.split("4.")[0]
                else:
                    moves = ""
            elif line.startswith("["):  # Game continues, keep appending
                game.append(re.findall(pattern, line)[0])
            elif not line.startswith("[") and moves is not None:
                # Game just ended, dump to NDJSON file
                if include_moves:
                    game.append(("Moves", moves))
                    #if "eval" in modes
                game_df = dict(game)
                game_df["Evaluation_flag"] = "eval" in moves
                game_df["Tournament"] = tournament


                for player in ["White", "Black"]:
                    id_player = game_df[player]

                    game_type = game_df["Event"]
                    if game_type not in d_cum_games:
                        d_cum_games[game_type] = defaultdict(int)

                    # Total games played
                    if id_player not in d_cum_games["All"]:
                        d_cum_games["All"][id_player] = 0
                        # Add random uniform number per player
                        d_cum_games["All"][f"{id_player}_random"] = random()

                    # Games played in category
                    if id_player not in d_cum_games[game_type]:
                        d_cum_games[game_type][id_player] = 0
                        d_cum_games[game_type][f"{id_player}Elo_max"] = 0
                        d_cum_games[game_type][f"{id_player}Elo_max_faced"] = 0
                    
                    # Add cumulative values to game
                    d_cum_games[game_type][id_player] += 1
                    d_cum_games["All"][id_player] += 1

                    game_df["ID_random"] =  random()
                    game_df[f"{player}_random"] =  d_cum_games["All"][f"{id_player}_random"]
                    game_df[f"{player}_cum_games_type"] =  d_cum_games[game_type][id_player]
                    game_df[f"{player}_cum_games_total"] = d_cum_games["All"][id_player]

                    # Find max Elo of each player
                    max_elo = d_cum_games[game_type][f"{id_player}Elo_max"]
                    if game_df[f"{player}Elo"] == "?":
                        game_df[f"{player}Elo_max"] = max_elo
                    elif int(game_df[f"{player}Elo"]) > max_elo:
                        d_cum_games[game_type][f"{id_player}Elo_max"] = int(game_df[f"{player}Elo"])
                        game_df[f"{player}Elo_max"] = int(game_df[f"{player}Elo"])
                    else:
                        game_df[f"{player}Elo_max"] = max_elo
                    
                    # Max ELO faced
                    max_elo = d_cum_games[game_type][f"{id_player}Elo_max_faced"]
                    if game_df[f"{player}Elo"] == "?":
                        game_df[f"{player}Elo_max_faced"] = max_elo
                    elif int(game_df[f"{player}Elo"]) > max_elo:
                        d_cum_games[game_type][f"{id_player}Elo_max_faced"] = int(game_df[f"{player}Elo"])
                        game_df[f"{player}Elo_max_faced"] = int(game_df[f"{player}Elo"])
                    else:
                        game_df[f"{player}Elo_max_faced"] = max_elo
                    

                # Add fields that are missing when they have no value
                for field in ['BlackTitle', 'WhiteTitle']:
                    if field not in game_df:
                        game_df.update({field: None})

                # Add concat DateTime field to replace seperate Date & TIme
                game_df.update({'DateTime': f"{game_df['UTCDate']} {game_df['UTCTime']}"})

                # Write complete game to temp file
                temp_file.write(json.dumps(game_df) + "\n")

                looking_for_game = True
                game = []
                moves = None
                games += 1

                if games >= ndjson_size:
                    if dir_ndjson is not None:
                        temp_file.close()

                    # Convert the NDJSON to Parquet
                    _ndjson_to_parquet(temp_file.name,
                                    f"{dir_parquet}/{year}_{month:02}_{batch:003}.parquet", include_moves, fs=fs)
                    batch += 1

                    # When the max size of ndjson is  reached, create new temp file
                    if dir_ndjson is None:
                        temp_file = TempFile(suffix=".ndjson", mode="w+")
                    else:
                        # useful for debugging
                        temp_file = open(f"{dir_ndjson}/temp.ndjson", mode="w+")
                    games = 0

        # Avoid 'hanging' progress bars due to approximation/actual value-mismatch
        progress_bar.update(num_bytes * 5.2)
        # Clean up
        progress_bar.close()
        # close file
        if dir_ndjson is not None:
            temp_file.close()

    # Last batch
    _ndjson_to_parquet(temp_file.name,
                    f"{dir_parquet}/{year}_{month:02}_{batch:003}.parquet", include_moves, fs=fs)

    # Save cumulative values to file
    compressed_bytes = zstd.ZstdCompressor().compress(json.dumps(d_cum_games).encode('utf-8'))
    if isinstance(fs, s3fs.core.S3FileSystem):
        # read cum_files from S3 point
        with fs.open(f"{dir_parquet}/cum_files.json.zst", mode='wb') as fout:
            fout.write(compressed_bytes)
    else:
        # Read and decompress the data
        with open(f"{dir_parquet}/cum_files.json.zst", 'wb') as fout:
            fout.write(compressed_bytes)

def _ndjson_to_parquet(ndjson_path: str, parquet_path: str, include_moves: bool, fs: Optional[s3fs.core.S3FileSystem] = None):
    """Creates a cleaned dataframe from an ndjson of Lichess game info."""
    game_cols = ["ID", "ID_random", "Event", "Tournament", "ECO", "Opening", "TimeControl", "Termination", "DateTime"]

    schema = {
              "Event": pl.Utf8,
              "Site": pl.Utf8,
              "ID_random": pl.Float64,
              "White": pl.Utf8, 
              "Black": pl.Utf8, 
              "Result":pl.Utf8, #pl.Enum(["1-0", "0-1", "1/2-1/2", "?", "*"]), 
              "WhiteElo": pl.Utf8, 
              "BlackElo": pl.Utf8, 
              "WhiteElo_max": pl.Int32, 
              "BlackElo_max": pl.Int32, 
              "WhiteElo_max_faced": pl.Int32, 
              "BlackElo_max_faced": pl.Int32, 
              "White_random": pl.Float64,
              "Black_random": pl.Float64,
              "White_cum_games_total": pl.Int32,
              "Black_cum_games_total": pl.Int32,
              "White_cum_games_type": pl.Int32,
              "Black_cum_games_type": pl.Int32,
              "WhiteTitle": pl.Utf8, 
              "BlackTitle": pl.Utf8, 
              "WhiteRatingDiff": pl.Utf8, 
              "BlackRatingDiff": pl.Utf8, 
              "ECO": pl.Utf8, 
              "Opening": pl.Utf8, 
              "TimeControl": pl.Utf8, 
              "Termination": pl.Utf8, #pl.Enum(["Time forfeit", "Rules infraction", "Normal", "Abandoned", "Unterminated", "?"]), 
              "DateTime": pl.Utf8,
              "Tournament": pl.Boolean
            }

    if include_moves:
        game_cols.append("Moves")
        schema["Moves"] = pl.Utf8
        game_cols.append("Evaluation_flag")
        schema["Evaluation_flag"] = pl.Boolean

    # Convert from UTF with "?" symbol to INT
    int_cols = ["WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff"]
    exclude_int = ["White_cum_games_total", "Black_cum_games_total", "WhiteElo_max", "BlackElo_max", "WhiteElo_max_faced", "BlackElo_max_faced",
                   "White_random", "Black_random", "ID_random", "White_cum_games_type", "Black_cum_games_type"]

    logging.debug("Creating dataframe")
    lf = (
        # create lazy dataframe
        pl.scan_ndjson(ndjson_path, schema=schema)
        # transform all ? values into nulls
        # see here: https://stackoverflow.com/a/74816042
        .with_columns(pl.when(pl.exclude(exclude_int) != "?").then(pl.exclude(exclude_int)))
        # now, do light data transformation
        .with_columns(
            pl.col(int_cols).str.replace(r"\+", "").cast(pl.Int16),
            pl.col("DateTime").str.to_datetime(format="%Y.%m.%d %H:%M:%S"),
            pl.col("Site").str.replace("https://lichess.org/", "").alias("ID"),
            # Title is not missing (flag)
            pl.col("WhiteTitle").is_not_null().alias("WhiteTitle_flag"),
            pl.col("BlackTitle").is_not_null().alias("BlackTitle_flag"),
        )
        # # lastly, select only what we need
        .select(
            *game_cols,
            "Result",
            pl.lit("White").alias("Role_player"),
            pl.col("White").alias("Player"),
            pl.col("Black").alias("Opponent"),
            pl.col("WhiteElo").alias("PlayerElo"),
            pl.col("BlackElo").alias("OpponentElo"),
            pl.col("WhiteElo_max").alias("PlayerElo_max"),
            pl.col("BlackElo_max").alias("OpponentElo_max"),
            pl.col("WhiteElo_max_faced").alias("PlayerElo_max_faced"),
            pl.col("BlackElo_max_faced").alias("OpponentElo_max_faced"),
            pl.col("WhiteTitle").alias("PlayerTitle"),
            pl.col("BlackTitle").alias("OpponentTitle"),
            pl.col("WhiteTitle_flag").alias("PlayerTitle_flag"),
            pl.col("BlackTitle_flag").alias("OpponentTitle_flag"),
            pl.col("WhiteRatingDiff").alias("PlayerRatingDiff"),
            pl.col("BlackRatingDiff").alias("OpponentRatingDiff"),
            pl.col("White_random").alias("Player_random"),
            pl.col("Black_random").alias("Opponent_random"),
            pl.col("White_cum_games_total").alias("Player_cum_games_total"),
            pl.col("Black_cum_games_total").alias("Opponent_cum_games_total"),
            pl.col("White_cum_games_type").alias("Player_cum_games_type"),
            pl.col("Black_cum_games_type").alias("Opponent_cum_games_type"),
        )
        .set_sorted("DateTime")
    )
    
    d_rev_result = {"1-0": "0-1", "0-1": "1-0", "1/2-1/2": "1/2-1/2", "?": "?", "*": "*"}
    # Convert to player-game-role format: i.e., duplicate each game switching White and Black
    lf_inv = lf.select(
            *game_cols,
            pl.col("Result").map_elements(d_rev_result.get, return_dtype=pl.Utf8),
            pl.lit("Black").alias("Role_player"),
            pl.col("Opponent").alias("Player"),
            pl.col("Player").alias("Opponent"),
            pl.col("OpponentElo").alias("PlayerElo"),
            pl.col("PlayerElo").alias("OpponentElo"),
            pl.col("OpponentElo_max").alias("PlayerElo_max"),
            pl.col("PlayerElo_max").alias("OpponentElo_max"),
            pl.col("OpponentElo_max_faced").alias("PlayerElo_max_faced"),
            pl.col("PlayerElo_max_faced").alias("OpponentElo_max_faced"),
            pl.col("OpponentTitle").alias("PlayerTitle"),
            pl.col("PlayerTitle").alias("OpponentTitle"),
            pl.col("OpponentTitle_flag").alias("PlayerTitle_flag"),
            pl.col("PlayerTitle_flag").alias("OpponentTitle_flag"),
            pl.col("OpponentRatingDiff").alias("PlayerRatingDiff"),
            pl.col("PlayerRatingDiff").alias("OpponentRatingDiff"),
            pl.col("Opponent_random").alias("Player_random"),
            pl.col("Player_random").alias("Opponent_random"),
            pl.col("Opponent_cum_games_total").alias("Player_cum_games_total"),
            pl.col("Player_cum_games_total").alias("Opponent_cum_games_total"),
            pl.col("Opponent_cum_games_type").alias("Player_cum_games_type"),
            pl.col("Player_cum_games_type").alias("Opponent_cum_games_type")
    ).set_sorted("DateTime")
    
    # concatenate files sorted by time (should be fast)
    lf = (lf
          .merge_sorted(lf_inv, key="DateTime")
          .sort(["DateTime", "ID"])
          .with_columns(
              pl.col("PlayerElo").cut(range(0, 4001, 200)).alias("PlayerElo_bin"),
              # Cast to enums (we could also cast titles and others)
              pl.col("Result").cast(pl.Enum(["1-0", "0-1", "1/2-1/2", "?", "*"])),
              pl.col("Termination").cast(pl.Enum(["Time forfeit", "Rules infraction", "Normal", "Abandoned", "Unterminated", "?"])),
              pl.col("Role_player").cast(pl.Enum(["White", "Black"])),
          )
    )

    logging.info("Writing '%s", parquet_path)
    if isinstance(fs, s3fs.core.S3FileSystem):
        # write parquet
        with fs.open(parquet_path, mode='wb') as f:
            lf.collect(streaming=True).write_parquet(f, compression='gzip', use_pyarrow=True)
    else:
        # gzip and use_pyarrow are required for default Apache Drill compatibility
        lf.collect(streaming=True).write_parquet(parquet_path, compression='gzip', use_pyarrow=True)

    return parquet_path
