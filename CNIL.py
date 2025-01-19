
"""
This script downloads, extracts, and processes XML files from the DILA base URL for CNIL data.
It performs the following steps:
1. Fetches and parses HTML content from a given URL to extract .tar.gz file names.
2. Downloads files from the given URL and saves them to a specified target path, displaying a progress bar.
3. Downloads a tar.gz file from a specified URL and extracts its contents to a given directory.
4. Parses a CNIL XML file and extracts relevant data.
5. Writes the results to a JSONL file.
"""

from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError
from contextlib import closing
from typing import List
from datetime import datetime
import xml.etree.ElementTree as ET
import glob
import re
import tarfile
import json
import logging
import requests

# https://tqdm.github.io/
from tqdm import tqdm

logging.basicConfig(
    filename=f'capp_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_tar_files(url: str) -> tuple[List[str], List[str]]:
    """
    Fetches and parses the HTML content from the given URL to extract .tar.gz file names.
    Args:
        url (str): The URL to fetch the HTML content from.
    Returns:
        tuple[List[str], List[str]]: A tuple containing a list of unique .tar.gz file names found in the HTML content.
    Raises:
        URLError: If there is an issue accessing the URL.
    """

    try:
        with closing(urlopen(url)) as response:
            html_content = response.read()
            if isinstance(html_content, bytes):
                html_content = html_content.decode("utf-8")

        tar_pattern = re.compile(r"[\w-]+\.tar\.gz")
        remote_files = remote_files = list(set(tar_pattern.findall(html_content)))
        return remote_files
    except URLError as e:
        raise URLError(f"Failed to access URL {url}: {str(e)}") from e


def download_with_progress(
    url: str, target_path: Path, chunk_size: int = 32768
) -> None:
    """
    Downloads a file from the given URL and saves it to the specified target path,
    displaying a progress bar during the download.
    Args:
        url (str): The URL of the file to download.
        target_path (Path): The local file path where the downloaded file will be saved.
        chunk_size (int, optional): The size of each chunk to read from the response. Defaults to 32768 bytes.
    Raises:
        requests.exceptions.RequestException: If there is an issue with the HTTP request.
    """

    response = requests.get(url, stream=True, timeout=15)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    progress = tqdm(total=total_size, unit="iB", unit_scale=True)

    try:
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                progress.update(len(chunk))
                f.write(chunk)
    finally:
        progress.close()


def download_and_extract(
    url: str,
    filename: str,
    extract_path: Path,
):
    """
    Downloads a tar.gz file from a specified URL and extracts its contents to a given directory.
    Args:
        url (str): The base URL from which to download the file.
        filename (str): The name of the file to download.
        extract_path (Path): The directory where the file should be extracted.
    Raises:
        requests.exceptions.RequestException: If there is an error during the download process.
        tarfile.TarError: If there is an error during the extraction process.
        RuntimeError: If an unexpected error occurs during the download or extraction process.
    """

    tar_path = Path(extract_path, filename)
    try:
        if not tar_path.exists():
            logging.info("Downloading %s...", filename)
            download_with_progress(f"{url}{filename}", tar_path)

            logging.info("Extracting %s...", filename)
            with tarfile.open(str(tar_path), mode="r:gz") as tar:
                tar.extractall(str(extract_path))
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"Download failed for {filename}: {str(e)}"
        ) from e
    except tarfile.TarError as e:
        raise tarfile.TarError(f"Tar extraction failed for {filename}: {str(e)}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error processing {filename}: {str(e)}") from e


def parse_cnil_xml_file(xml_path):
    """
    Parses a CNIL XML file and extracts relevant data.
    Args:
        xml_path (str): The path to the XML file to be parsed.
    Returns:
        dict: A dictionary containing the extracted data with keys such as 'id', 'origine', 'url', 'nature',
              'titrefull', 'numero', 'nature_delib', 'date_texte', 'date_publi', 'etat_juridique', and 'contenu'.
    Raises:
        ValueError: If the XML file has an invalid format.
        RuntimeError: If any other error occurs during parsing.
    """

    data_extracted = {}

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        meta = root.find(".//META_COMMUN")
        if meta is not None:
            for field in ["ID", "ORIGINE", "URL", "NATURE"]:
                data_extracted[field.lower()] = meta.findtext(field, "")

        meta = root.find(".//META_CNIL")
        for field in [
            "TITREFULL",
            "NUMERO",
            "NATURE_DELIB",
            "DATE_TEXTE",
            "DATE_PUBLI",
            "ETAT_JURIDIQUE",
        ]:
            data_extracted[field.lower()] = meta.findtext(field, "")

        contenu = root.find(".//CONTENU")
        if contenu is not None:
            data_extracted["contenu"] = ET.tostring(
                contenu, encoding="unicode", method="text"
            ).strip()

        return data_extracted
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML format in {xml_path}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Error parsing {xml_path}: {e}") from e


def tar_dila_data(base):
    """
    Downloads, extracts, processes XML files from a specified DILA base, and writes the results to a JSONL file.
    Args:
        base (str): The base name to construct the DILA base URL.
    The function performs the following steps:
    1. Creates the necessary directory structure for storing the extracted data.
    2. Constructs the DILA base URL using the provided base name.
    3. Downloads and extracts tar files from the DILA base URL, skipping files containing "Freemium".
    4. Processes the extracted XML files to parse relevant data.
    5. Writes the parsed data to a JSONL file named "cnil_dataset.jsonl".
    Note:
        - The function assumes the existence of helper functions: `get_tar_files`, `download_and_extract`, and `parse_cnil_xml_file`.
        - The function uses the `tqdm` library for progress indication.
    """

    dila_data_path = Path(r"data\dila", "extract", "cnil")
    dila_data_path.mkdir(parents=True, exist_ok=True)

    dila_base_url = f"https://echanges.dila.gouv.fr/OPENDATA/{base}/"

    logging.info("Downloading and extracting files...")
    remote_files = get_tar_files(dila_base_url)
    for remote_file in tqdm(remote_files):
        if "Freemium" in remote_file:
            continue
        download_and_extract(dila_base_url, remote_file, dila_data_path)

    logging.info("Processing XML files...")
    results = []
    xml_files = glob.glob(f"{dila_data_path}/**/*.xml", recursive=True)
    for xml_file in tqdm(xml_files):
        data = parse_cnil_xml_file(xml_file)
        if data:
            results.append(data)

    logging.info("Writing results to 'cnil_dataset.jsonl'")
    with open("cnil_dataset.jsonl", "w", encoding="utf-8") as f:
        for item in results:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    tar_dila_data("CNIL")
