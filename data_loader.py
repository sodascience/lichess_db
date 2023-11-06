"""Module to convert .pgn file headers to parquet files."""
from pathlib import Path
import polars as pl
from tqdm import tqdm
from chess.pgn import read_headers


class LichessDataLoader:
    def __init__(self, parquet_dir: Path | str | None) -> None:
        """Initialize the downloader with a specific output folder."""
        self.parquet_dir = Path(parquet_dir) if parquet_dir is not None else Path()
        if not self.parquet_dir.is_dir():
            raise ValueError("Argument parquet_dir should be directory!")
        self.pgn_file = None
        self.pgn_len = 0
        self.data_list = []

    def convert_pgn(self, pgn_path: Path | str, chunk_size=int | None) -> Path:
        pgn_path = Path(pgn_path)
        # first, connect to the pgn file
        self._connect_pgn_file(pgn_path)

        if chunk_size is None:
            # read and convert the whole file
            self._read_chunk(chunk_size=self.pgn_len)
            df = self._convert_to_polars()
            out_path = self.parquet_dir / f"{pgn_path.stem}.parquet"
            df.write_parquet(out_path)
            self._empty_data_list()
            return out_path

        raise NotImplementedError("Chunked reading not yet implemented!")

    def _connect_pgn_file(self, pgn_path: Path | str) -> None:
        pgn_path = Path(pgn_path)
        if not pgn_path.is_file() or pgn_path.suffix != ".pgn":
            raise ValueError("Path {pgn_path} is not a pgn file!")
        # get the total length of the pgn file
        with pgn_path.open("r") as f:
            self.pgn_len = sum(1 for _ in f if "Event" in _)
        self.pgn_file = pgn_path.open("r")

    def _read_chunk(self, chunk_size: int = 1024) -> None:
        chunk_len = min(chunk_size, self.pgn_len)
        for _ in tqdm(range(chunk_len)):
            chess_header = read_headers(self.pgn_file)
            self.data_list.append(dict(chess_header))
            self.pgn_len -= 1

    def _empty_data_list(self) -> None:
        self.data_list = []

    def _convert_to_polars(self) -> pl.DataFrame:
        """Creates a cleaned dataframe from a list of lichess header dicts."""
        return (
            # create dataframe
            pl.DataFrame(self.data_list)
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
                    "Event",
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
