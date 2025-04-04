import argparse
import logging
import requests
import csv
import os
from io import StringIO
from typing import List, Tuple
from urllib.parse import urlparse
from datetime import datetime
from http.client import IncompleteRead
from requests.exceptions import RequestException
from bs4 import BeautifulSoup

def main() -> None:
    """
    Main function to download ERDDAP datasets based on user-specified parameters.
    """
    parser = argparse.ArgumentParser(description="Download ERDDAP datasets.")
    parser.add_argument(
        "--erddap-urls",
        type=str,
        required=True,
        help="Comma-separated list of ERDDAP URLs.",
    )
    parser.add_argument(
        "--formats",
        type=str,
        default="ncCF,das",
        help="Download these formats for every dataset.",
    )
    parser.add_argument(
        "--datasetIDs",
        type=str,
        help="Comma-separated list of dataset IDs to download. Can only be specified if there is one ERDDAP URL.",
    )
    parser.add_argument(
        "--downloads-folder",
        type=str,
        default="downloads",
        help="Folder to save the downloaded files.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip downloading files that already exist in the download folder.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )
    parser.add_argument(
        "--grid-datasets-1", "-g", action="store_true", help="Include grid datasets with files (only downloads source files)."
    )
    parser.add_argument("--grid-datasets-2", "-h", action="store_true", help="Include grid datasets without files (downloads based on formats)." )
    parser.add_argument(
        "--table-datasets", "-t", action="store_true", help="Include table datasets."
    )

    args = parser.parse_args()

    # Set the logging level based on the command-line argument
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    erddap_urls = args.erddap_urls.split(",")
    formats = args.formats.split(",")
    error_report = []

    if args.datasetIDs and len(erddap_urls) > 1:
        logger.error("datasetIDs can only be specified if there is one ERDDAP URL.")
        return

    if not args.table_datasets and not args.grid_datasets_1 and not args.grid_datasets_2:
        logger.error(
            "At least one of --table-datasets (-t) or --grid-datasets-1 (-g) or --grid-datasets-2 (-h) must be selected."
        )
        return

    start_time = datetime.now()

    for erddap_url in erddap_urls:
        logger.info(f"Processing ERDDAP URL: {erddap_url}")
        erddap_url_no_protocol_no_path = urlparse(erddap_url).netloc

        dataset_ids = get_dataset_ids(erddap_url, args.datasetIDs, logger)

        for dataset_id, data_structure, file_url, iso19115_url in dataset_ids:
            download_dir = os.path.join(
                args.downloads_folder, erddap_url_no_protocol_no_path, dataset_id
            )
            os.makedirs(download_dir, exist_ok=True)

            if args.skip_existing and all_formats_exist(
                download_dir, dataset_id, formats, logger
            ):
                logger.debug(
                    f"All formats for datasetID {dataset_id} already exist. Skipping."
                )
                continue

            is_table = data_structure == "table"
            has_files = file_url and file_url != ""
            did_download = False
            if is_table and args.table_datasets:
                did_download = True
                error_report.extend(
                    download_dataset_files(
                        erddap_url,
                        dataset_id,
                        formats,
                        download_dir,
                        args.skip_existing,
                        logger,
                    )
                )
            elif not is_table and has_files and args.grid_datasets_1:
                did_download = True
                error_report.extend(download_files(erddap_url, dataset_id, file_url, download_dir, logger))
            elif not is_table and not has_files and args.grid_datasets_2:
                did_download = True
                logger.error("not implemented yet")

            # If file was to be downloaded, then download the ISO 19115 for it
            if (did_download and iso19115_url and iso19115_url != ""):
                iso19115_file_path = os.path.join(download_dir, f"{dataset_id}.iso19115")
                download(iso19115_url, iso19115_file_path, logger)

    do_error_report(error_report, args.downloads_folder, start_time, logger)

def download_files(erddap_url: str, dataset_id: str, file_url: str, download_dir: str, logger: logging.Logger) -> List[Tuple[str, str, str, Exception]]:
    """
    Download files for a dataset from the specified URL.

    Args:
        erddap_url (str): The base URL of the ERDDAP server.
        dataset_id (str): The ID of the dataset.
        file_url (str): The URL to fetch file names from.
        download_dir (str): The directory where files are downloaded.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        List[Tuple[str, str, str, Exception]]: A list of tuples containing the ERDDAP URL, dataset ID, file name, and error for missed downloads.
    """
    missed_files = []
    try:
        file_names = extract_file_names_from_url(file_url)
    except (RequestException, IncompleteRead) as e:
        logger.error(f"Failed to fetch files for datasetID {dataset_id}. Error: {e}")
        missed_files.append((erddap_url, dataset_id, "all", e))
        return missed_files

    for file_name in file_names:
        url = f"{file_url}{file_name}"
        file_path = os.path.join(download_dir, f"{file_name}")
        try:
            download(url, file_path, logger)
        except (RequestException, IncompleteRead) as e:
            logger.error(
                f"Failed to fetch file {file_name} for datasetID {dataset_id}. Error: {e}"
            )
            missed_files.append((erddap_url, dataset_id, file_name, e))
    return missed_files

def extract_file_names_from_url(file_url: str) -> List[str]:
    """
    Extract file names from the specified URL.

    Args:
        file_url (str): The URL to fetch file names from.

    Returns:
        List[str]: A list of file names extracted from the URL.
    """
    # Fetch the HTML content from the URL
    response = requests.get(file_url)
    response.raise_for_status()  # Check for HTTP errors

    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all <tr> elements with an <img> tag with alt="[BIN]"
    file_locations = []
    for row in soup.find_all("tr"):
        img_tag = row.find("img", alt="[BIN]")
        if img_tag:
            href_tag = row.find_all("td")[1].find("a")
            if href_tag:
                file_locations.append(href_tag["href"])

    return file_locations

def extract_grid_variables_from_url(file_url: str) -> List[str]:
    """
    Extract grid variables from the specified URL.

    Args:
        file_url (str): The URL to fetch grid variables from.

    Returns:
        List[str]: A list of grid variable values extracted from the URL.
    """
    response = requests.get(file_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table where the first row contains "Grid Variables"
    tables = soup.find_all('table')
    target_table = None

    for table in tables:
        first_row = table.find('tr')
        if first_row and "Grid Variables" in first_row.get_text():
            target_table = table
            break

    if not target_table:
        return []

    # Extract values from rows 2..n
    values = []
    rows = target_table.find_all('tr')[1:]  # Skip the first row
    for row in rows:
        td = row.find('td')
        input_tag = td.find('input')
        if input_tag and 'value' in input_tag.attrs:
            values.append(input_tag['value'])

    return values

def get_dataset_ids(
    erddap_url: str, specified_dataset_ids: str, logger: logging.Logger
) -> List[Tuple[str, str, str, str]]:
    """
    Fetch dataset IDs from the ERDDAP server or use specified dataset IDs.

    Args:
        erddap_url (str): The base URL of the ERDDAP server.
        specified_dataset_ids (str): Comma-separated list of dataset IDs specified by the user.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        List[Tuple[str, str, str, str]]: A list of dataset IDs to download.
    """
    specified_datasets = (
        specified_dataset_ids.split(",") if specified_dataset_ids else None
    )

    datasets_url = f"{erddap_url}/tabledap/allDatasets.csv?datasetID%2CdataStructure%2Cfiles%2Ciso19115"
    response = requests.get(datasets_url)
    response.raise_for_status()

    dataset_ids = []
    csv_data = StringIO(response.text)
    csv_reader = csv.reader(csv_data)
    header = next(csv_reader)
    dataset_id_index = header.index("datasetID")
    data_structure_index = header.index("dataStructure")
    files_index = header.index("files")
    iso19115_index = header.index("iso19115")
    for row in csv_reader:
        dataset_id = row[dataset_id_index]
        data_structure = row[data_structure_index]
        file_url = row[files_index]
        iso19115_url = row[iso19115_index]
        if (
            dataset_id != "allDatasets"
            and data_structure
            and (not specified_datasets or dataset_id in specified_datasets)
        ):
            dataset_ids.append((dataset_id, data_structure, file_url, iso19115_url))
    logger.debug(f"Fetched Dataset IDs: {dataset_ids}")
    return dataset_ids

def all_formats_exist(
    download_dir: str, dataset_id: str, formats: List[str], logger: logging.Logger
) -> bool:
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
        os.path.exists(os.path.join(download_dir, f"{dataset_id}.{fmt}"))
        for fmt in formats
    )

def download_dataset_files(
    erddap_url: str,
    dataset_id: str,
    formats: List[str],
    download_dir: str,
    skip_existing: bool,
    logger: logging.Logger,
) -> List[Tuple[str, str, str, Exception]]:
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
        List[Tuple[str, str, str, Exception]]: A list of tuples containing the ERDDAP URL, dataset ID, format, and error for missed downloads.
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
            logger.error(
                f"Failed to fetch data for datasetID {dataset_id} in format {fmt}. Error: {e}"
            )
            missed_formats.append((erddap_url, dataset_id, fmt, e))
    return missed_formats

def download(url: str, file_path: str, logger: logging.Logger) -> None:
    """
    Download a file from the specified URL and save it to the file path.

    Args:
        url (str): The URL to download the file from.
        file_path (str): The path to save the downloaded file.
        logger (logging.Logger): Logger for logging messages.
    """
    response = requests.get(url)
    response.raise_for_status()

    with open(file_path, "wb") as file:
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

def do_error_report(
    missed_formats: List[Tuple[str, str, str, Exception]],
    downloads_folder: str,
    start_time: datetime,
    logger: logging.Logger,
) -> None:
    """
    Report missed formats to a CSV file.

    Args:
        missed_formats (List[Tuple[str, str, str, Exception]]): A list of tuples containing the ERDDAP URL, dataset ID, format/file name, and error for missed downloads.
        downloads_folder (str): The folder where the missed formats file will be saved.
        start_time (datetime): The time when the script was started.
        logger (logging.Logger): Logger for logging messages.
    """
    report_file = os.path.join(
        downloads_folder, "missed_formats-{date:%Y-%m-%d_%H:%M:%S}.csv"
    ).format(date=datetime.now())
    file_exists = os.path.exists(report_file)

    with open(report_file, mode="a", newline="") as file:
        writer = csv.writer(file)

        # Write the header if the file is new
        if not file_exists:
            writer.writerow(["time", "erddap_url", "datasetID", "missed", "error"])

        # Write the missed formats
        for erddap_url, dataset_id, missed, e in missed_formats:
            writer.writerow([start_time.isoformat(), erddap_url, dataset_id, missed, e])

    if missed_formats:
        logger.info(
            f"Some content was missed. Details have been written to {report_file}"
        )

if __name__ == "__main__":
    main()
