"""Module to download and decompress lichess database files."""
from pathlib import Path
import requests
from tqdm import tqdm
import zstandard as zstd


class LichessDownloader:
    """Light class to download lichess files to local directory."""

    def __init__(self, data_folder: Path | str | None) -> None:
        """Initialize the downloader with a specific output folder."""
        self.data_folder = Path(data_folder) if data_folder is not None else Path()
        if not self.data_folder.is_dir():
            raise ValueError("Argument data_folder should be directory!")

    def download_file(self, url: str) -> Path:
        """Download a file to the directory."""
        # code from https://stackoverflow.com/a/37573701
        file_name = Path(url).name
        res = requests.get(url, stream=True)
        if res.status_code != 200:
            res.raise_for_status()  # Will only raise for 4xx codes, so...
            raise RuntimeError(
                f"Request to {url} returned status code {res.status_code}"
            )
        num_bytes = int(res.headers.get("content-length", 0))

        # actually download with progress bar
        progress_bar = tqdm(total=num_bytes, unit="iB", unit_scale=True)
        with open(self.data_folder / file_name, "wb") as file:
            for data in res.iter_content(1024):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()

        # return the file path
        return self.data_folder / file_name

    def download_month(self, year: int, month: int) -> Path:
        pass


def decompress_file(input_file: Path | str, output_dir: Path | str | None):
    """Function to decompress .zst file"""
    # check args
    input_file = Path(input_file)
    if output_dir is None:
        output_dir = input_file.parent
    else:
        output_dir = Path(output_dir)

    if not output_dir.is_dir():
        raise ValueError("output_dir should be a directory!")

    # do the decompression
    decompressor = zstd.ZstdDecompressor()
    with input_file.open("rb") as compressed:
        with (output_dir / input_file.stem).open("wb") as decompressed:
            decompressor.copy_stream(compressed, decompressed)

    return output_dir / input_file.stem
