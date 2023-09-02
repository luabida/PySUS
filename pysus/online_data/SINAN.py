import pandas as pd
from pathlib import Path
from typing import Union
from pysus.online_data import CACHEPATH
from pysus.ftp.databases import SINAN


sinan = SINAN()


def list_diseases() -> dict:
    """List available diseases on SINAN"""
    return sinan.diseases


def get_available_years(disease_code: str) -> list:
    """
    Fetch available years for data related to specific disease
    :param disease_code: Disease code. See `SINAN.list_diseases` for valid codes
    :return: A list of DBC files from a specific disease found in the FTP Server.
    """
    return sinan.get_files(dis_codes=disease_code)


def download(
    diseases: Union[str, list],
    years: Union[str, list, int],
    data_path: str = CACHEPATH,
) -> list:
    """
    Downloads SINAN data directly from Datasus ftp server.
    :param disease: Disease according to `agravos`.
    :param years: 4 digit integer, can be a list of years.
    :param data_path: The directory where the file will be downloaded to.
    :return: list of downloaded files.
    """
    downloaded = []
    files = sinan.get_files(dis_codes=diseases, years=years)
    for file in files:
        downloaded.append(file.download(local_dir=data_path))
    return downloaded


def metadata_df(disease_code: str) -> pd.DataFrame:
    metadata_file = (
        Path(__file__).parent.parent
        / "metadata"
        / "SINAN"
        / f"{disease_code}.tar.gz"
    )
    if metadata_file.exists():
        df = pd.read_csv(
            metadata_file,
            compression="gzip",
            header=0,
            sep=",",
            quotechar='"',
            error_bad_lines=False,
        )

        return df.iloc[:, 1:]
    else:
        print(f"No metadata available for {disease}")
        return
