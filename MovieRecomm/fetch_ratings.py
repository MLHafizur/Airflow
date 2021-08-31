import logging
from pathlib import Path
import tempfile
from urllib.request import urlretrieve
import zipfile

import click
import pandas as pd

logging.basicConfig(
    format="[%(asctime)-15s] %(levelname)s - %(message)s", level=logging.INFO
)

@click.command()
@click.argument("--start_date", default='2020-01-01', type=click.DateTime())
@click.argument("--end_date", default='2021-08-01', type=click.DateTime())
@click.argument("--output_path", required=True)

def fetch_ratings():
    """
    Fetching ratings from the url """
    url = "http://files.grouplens.org/datasets/movielens/ml-25m.zip"
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir, "download.zip")
        urlretrieve(url, tmp_file)

        with zipfile.ZipFile(tmp_file) as zip_:
            with zip_.open("ml-25m/ratings.csv") as file_:
                ratings = pd.read_csv(file_)
    
    return ratings


def main(start_date, end_date, output_path):
    """
    Fetching ratings from the url
    """
    ratings = fetch_ratings()
    ts_parsed = pd.to_datetime(ratings["timestamp"], unit="s")
    ratings = ratings.loc[(ts_parsed >= start_date) & (ts_parsed < end_date)]

    ratings.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
    