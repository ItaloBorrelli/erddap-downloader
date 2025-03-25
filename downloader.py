import argparse
import logging
import requests
import csv
import re
import os
from io import StringIO
from typing import List, Tuple
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib.parse import quote, urlparse
from datetime import datetime
from http.client import IncompleteRead
from requests.exceptions import RequestException
from bs4 import BeautifulSoup

def main():
    parser = argparse.ArgumentParser(description="Download ERDDAP datasets.")
    parser.add_argument("--erddap-urls", type=str, required=True, help="Comma-separated list of ERDDAP URLs.")
    parser.add_argument("--formats", type=str, default="ncCF,das,iso19115", help="Download these formats for every dataset.")
    parser.add_argument("--datasetIDs", type=str, help="Comma-separated list of dataset IDs to download. Can only be specified if there is one ERDDAP URL.")
    parser.add_argument("--downloads-folder", type=str, default="downloads", help="Folder to save the downloaded files.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files that already exist in the download folder.")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    parser.add_argument("--grid-datasets", "-g", action="store_true", help="Include grid datasets.")
    parser.add_argument("--table-datasets", "-t", action="store_true", help="Include table datasets.")

    args = parser.parse_args()

    # Set the logging level based on the command-line argument
    logging.basicConfig(level=getattr(logging, args.log_level), format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    erddap_urls = args.erddap_urls.split(',')
    formats = args.formats.split(',')
    missed_formats = []

    if args.datasetIDs and len(erddap_urls) > 1:
        logger.error("datasetIDs can only be specified if there is one ERDDAP URL.")
        return

    if not args.table_datasets and not args.grid_datasets:
        logger.error("One of --table-datasets (-t) or --grid-datasets (-g) must be selected.")
        return

    start_time = datetime.now()

    for erddap_url in erddap_urls:
        logger.info(f"Processing ERDDAP URL: {erddap_url}")
        erddap_url_no_protocol_no_path = urlparse(erddap_url).netloc

        dataset_ids = get_dataset_ids(erddap_url, args.datasetIDs, logger)

        for (dataset_id, is_table) in dataset_ids:
            download_dir = os.path.join(args.downloads_folder, erddap_url_no_protocol_no_path, dataset_id)
            os.makedirs(download_dir, exist_ok=True)

            if args.skip_existing and all_formats_exist(download_dir, dataset_id, formats, logger):
                logger.debug(f"All formats for datasetID {dataset_id} already exist. Skipping.")
                continue

            if is_table and args.table_datasets:
                missed_formats.extend(download_dataset_files(erddap_url, dataset_id, formats, download_dir, args.skip_existing, logger))
            elif not is_table and args.grid_datasets:
                file_url = f"{erddap_url}/files/{dataset_id}"
                file_locations = extract_file_locations_from_url(file_url)
                for file_location in file_locations:
                    url = f"{file_url}/{file_location}"
                    file_path = os.path.join(download_dir, f"{file_location}")
                    download(url, file_path, logger)



    report_missed_formats(missed_formats, args.downloads_folder, start_time, logger)

def extract_file_locations_from_url(file_url):
    # Fetch the HTML content from the URL
    response = requests.get(file_url)
    response.raise_for_status()  # Check for HTTP errors

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <tr> elements with an <img> tag with alt="[BIN]"
    file_locations = []
    for row in soup.find_all('tr'):
        img_tag = row.find('img', alt='[BIN]')
        if img_tag:
            href_tag = row.find_all('td')[1].find('a')
            if href_tag:
                file_locations.append(href_tag['href'])

    return file_locations

def get_dataset_ids(erddap_url: str, specified_dataset_ids: str, logger: logging.Logger) -> List[Tuple[str, bool]]:
    """
    Fetch dataset IDs from the ERDDAP server or use specified dataset IDs.

    Args:
        erddap_url (str): The base URL of the ERDDAP server.
        specified_dataset_ids (str): Comma-separated list of dataset IDs specified by the user.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        List[Tuple[str, bool]]: A list of dataset IDs to download.
    """
    specified_datasets = specified_dataset_ids.split(',') if specified_dataset_ids else None

    datasets_url = f"{erddap_url}/tabledap/allDatasets.csv?datasetID%2CdataStructure"
    response = requests.get(datasets_url)
    response.raise_for_status()

    dataset_ids = []
    csv_data = StringIO(response.text)
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        if row and row[0] and row[1] and row[0] != 'allDatasets' and (not specified_datasets or row[0] in specified_datasets):
            dataset_ids.append((row[0], row[1] == 'table'))
    logger.debug(f"Fetched Dataset IDs: {dataset_ids}")
    return dataset_ids

def all_formats_exist(download_dir: str, dataset_id: str, formats: List[str], logger: logging.Logger) -> bool:
    """
    Check if all required formats for a dataset already exist in the download directory.

    Args:
        download_dir (str): The directory where files are downloaded.
        dataset_id (str): The ID of the dataset.
        formats (List[str]): List of required formats.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        bool: True if all formats exist, False otherwise.
    """
    return all(
        os.path.exists(os.path.join(download_dir, f"{dataset_id}.{fmt}")) for fmt in formats
    )

def download_dataset_files(erddap_url: str, dataset_id: str, formats: List[str], download_dir: str, skip_existing: bool, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """
    Download dataset files in specified formats.

    Args:
        erddap_url (str): The base URL of the ERDDAP server.
        dataset_id (str): The ID of the dataset.
        formats (List[str]): List of required formats.
        download_dir (str): The directory where files are downloaded.
        skip_existing (bool): Whether to skip existing files.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        List[Tuple[str, str, str]]: A list of tuples containing the ERDDAP URL, dataset ID, and format for missed downloads.
    """
    missed_formats = []
    for fmt in formats:
        file_path = os.path.join(download_dir, f"{dataset_id}.{fmt}")
        if skip_existing and os.path.exists(file_path):
            logger.debug(f"Skipping existing file: {file_path}")
            continue

        url = build_dataset_url(erddap_url, dataset_id, fmt)
        try:
            download(url, file_path, logger)
        except (RequestException, IncompleteRead) as e:
            logger.error(f"Failed to fetch data for datasetID {dataset_id} in format {fmt}. Error: {e}")
            missed_formats.append((erddap_url, dataset_id, fmt))
    return missed_formats

def download(url, file_path, logger):
    response = requests.get(url)
    response.raise_for_status()

    with open(file_path, 'wb') as file:
        file.write(response.content)
    logger.debug(f"Saved data to {file_path}")

def build_dataset_url(erddap_url: str, dataset_id: str, fmt: str) -> str:
    """
    Build the URL to download a dataset in a specific format.

    Args:
        erddap_url (str): The base URL of the ERDDAP server.
        dataset_id (str): The ID of the dataset.
        fmt (str): The format to download.

    Returns:
        str: The constructed URL.
    """
    return f"{erddap_url}/tabledap/{dataset_id}.{fmt}"

def report_missed_formats(missed_formats: List[Tuple[str, str, str]], downloads_folder: str, start_time: datetime, logger: logging.Logger) -> None:
    """
    Report missed formats to a CSV file.

    Args:
        missed_formats (List[Tuple[str, str, str]]): A list of tuples containing the ERDDAP URL, dataset ID, and format for missed downloads.
        downloads_folder (str): The folder where the missed formats file will be saved.
        start_time (datetime): The time when the script was started.
        logger (logging.Logger): Logger for logging messages.
    """
    missed_formats_file = os.path.join(downloads_folder, "missed_formats.csv")
    file_exists = os.path.exists(missed_formats_file)

    with open(missed_formats_file, mode='a', newline='') as file:
        writer = csv.writer(file)

        # Write the header if the file is new
        if not file_exists:
            writer.writerow(["time", "erddap_url", "datasetID", "format"])

        # Write the missed formats
        for erddap_url, dataset_id, fmt in missed_formats:
            writer.writerow([start_time.isoformat(), erddap_url, dataset_id, fmt])

    if missed_formats:
        logger.info(f"Some formats were missed. Details have been written to {missed_formats_file}")

if __name__ == "__main__":
    main()
