#!/usr/bin/env python
# coding: utf-8

# In[10]:


import io
import json
import re
import requests
from tqdm.notebook import tqdm
import zstandard as zstd
import polars as pl


# In[11]:


def _convert_to_polars(path) -> pl.DataFrame:
    """Creates a cleaned dataframe from a list of lichess header dicts."""
    return (
        # create dataframe
        pl.read_ndjson(path)
        # transform all ? values into nulls
        # see here: https://stackoverflow.com/a/74816042
        .with_columns(pl.when(pl.all() != "?").then(pl.all()))
        # now, do light data transformation
        .with_columns(
            pl.when(pl.col("Result") == "0-1")
            .then(pl.lit("Black"))
            .otherwise(pl.lit("White"))
            .cast(pl.Categorical)
            .alias("Winner"),
            pl.col(
                ["WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff"]
            ).cast(pl.Int32),
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
                "Winner",
                "White",
                "Black",
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


# In[14]:


# Data url
def read_and_process(year, month, temp_file="temp.ndjson", dir_parquet="../lichess_parquet", size_parquet=10_000_000):
    """
    Download, process, and convert chess games data from the Lichess database to Parquet format.

    This function streams a dataset from the Lichess database, extracts chess game data,
    and writes it to a temporary NDJSON file. Once a certain number of games are processed,
    the data is converted to a Polars dataframe and saved to a Parquet file. This process repeats
    until all games in the dataset have been processed and saved.

    Parameters:
        year (int): The year of the dataset to download.
        month (str): The month of the dataset to download.
        temp_file (str, optional): Path to the temporary NDJSON file used for processing. Defaults to "temp.ndjson".
        dir_parquet (str, optional): Directory where Parquet files will be saved. Defaults to "../lichess_parquet".
        size_parquet (int, optional): Number of games to process before saving to a Parquet file. Defaults to 10,000,000.

    The function constructs a URL to stream the dataset from, uses regular expressions to parse the data,
    and utilizes Zstandard for decompression. Progress is tracked and displayed using a progress bar.

    The final dataset is written into the `dir_parquet` directory with a filename based on the `year`, `month`, 
    and a sequential number indicating the split of Parquet files.

    Example:
        read_and_process(2023, '01') # This will process games from January 2023.
    """
    # Your function implementation...

    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month}.pgn.zst"
    
    # Pattern to split lines with format [key\s"info"]
    pattern = re.compile('\[(.*?)\s"(.*)"\]')

    # Set up the decompressor
    decompressor = zstd.ZstdDecompressor(max_window_size=2**31)
    
    # Make a get request with streaming enabled. Open a temporary file
    with requests.get(url, stream=True) as response, open(temp_file, "w+") as fout:
        num_games_processed = 1
        num_bytes = int(response.headers.get("content-length", 0))
        # Decompress the response on the fly and read it line by line
        reader = decompressor.stream_reader(response.raw)
        text_stream = io.TextIOWrapper(reader, encoding='utf-8')
    
        num_files = 0
        looking_for_game = True
        game = []
        
        # Start progres bar (approximate bytes since the raw file is compressed and we are uncompressing on the fly)
        progress_bar = tqdm(total=num_bytes*5.2, unit="iB", unit_scale=True, miniters=100)
        for line in text_stream:
            progress_bar.update(len(line))
            if looking_for_game: # Looking for the start of the game
                if line.startswith("["):
                    looking_for_game = False
            else:
                if not line.startswith("["): # Game just ended, dump to NDJSON file
                    looking_for_game = True
                    fout.write(json.dumps(dict(game))+"\n")
                    num_games_processed += 1
                    game = []
                else: # Game continues, keep appending
                    game.append(re.findall(pattern,line)[0])
    
            #Process every 10M lines
            if (num_games_processed % size_parquet) == 0:
                fout.flush()
                df = _convert_to_polars(temp_file)
                df.to_parquet(f"{dir_parquet}/{year}_{month}_{num_files}.parquet")
                fout.seek(0)  # Seek to the beginning of the file
                fout.truncate()  # Clear the contents of the file
                num_files += 1
    
        # Clean up
        progress_bar.close()

    # Leftover games
    df = _convert_to_polars(temp_file)
    df.write_parquet(f"{dir_parquet}/{year}_{month}_{num_files}.parquet")


# In[15]:


read_and_process("2013", "01")


# In[ ]:




