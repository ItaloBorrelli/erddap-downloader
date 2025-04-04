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
from requests.exceptions import RequestException, HTTPError
from bs4 import BeautifulSoup


def main() -> None:
    """Main function to download ERDDAP datasets based on user-specified parameters."""
    parser = argparse.ArgumentParser(description="Download ERDDAP datasets.")
    parser.add_argument(
        "--erddap-urls",
        type=str,
        required=True,
        help="Comma-separated list of ERDDAP URLs.",
    )
    parser.add_argument(
        "--downloads-folder",
        type=str,
        default="downloads",
        help="Folder to save the downloaded files.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    erddap_urls = args.erddap_urls.split(",")
    for erddap_url in erddap_urls:
        erddap_url_no_protocol_no_path = urlparse(erddap_url).netloc
        download_dir = os.path.join(
            args.downloads_folder, erddap_url_no_protocol_no_path
        )
        os.makedirs(download_dir, exist_ok=True)
        file_url = f"{erddap_url}/files/"
        folders = extract_folder_names_from_url(file_url, logger)
        for folder in folders:
            new_url = f"{file_url}{folder}"
            new_download_location = os.path.join(download_dir, folder)
            os.makedirs(download_dir, exist_ok=True)
            download_files(erddap_url, new_url, new_download_location, logger)


def download_files(
    erddap_url: str,
    file_url: str,
    download_dir: str,
    logger: logging.Logger,
) -> List[Tuple[str, str, str, Exception]]:
    """Download files for a dataset from the specified URL."""
    missed_files = []
    file_names = extract_file_names_from_url(file_url, logger)
    folder_names = extract_folder_names_from_url(file_url, logger)

    for file_name in file_names:
        url = f"{file_url}{file_name}"
        file_path = os.path.join(download_dir, file_name)
        download(url, file_path, logger)
    for folder_name in folder_names:
        new_url = f"{file_url}{folder_name}"
        new_download_location = os.path.join(download_dir, folder_name)
        os.makedirs(download_dir, exist_ok=True)
        logger.debug(folder_name)
        download_files(erddap_url, new_url, new_download_location, logger)
    return missed_files

def extract_file_names_from_url(file_url: str, logger: logging.Logger) -> List[str]:
    """Extract file names from the specified URL."""
    response = requests.get(file_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    file_locations = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) > 0:
            img_tag = tds[0].find("img")
            if img_tag:
                href_tag = tds[1].find("a")
                if href_tag and "bookmark" in href_tag.get("rel", ""):
                    file_locations.append(href_tag["href"])

    return file_locations

def extract_folder_names_from_url(file_url: str, logger: logging.Logger) -> List[str]:
    """Extract file names from the specified URL."""
    response = requests.get(file_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    folder_locations = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) > 0:
            img_tag = tds[0].find("img")
            if img_tag:
                href_tag = tds[1].find("a")
                if href_tag:
                    if not (href_tag and "bookmark" in href_tag.get("rel", "")):
                        link_text = href_tag.get_text(strip=True)
                        if link_text != "Parent Directory":
                            folder_locations.append(href_tag["href"])

    logger.debug(folder_locations)
    return folder_locations


def extract_grid_variables_from_url(file_url: str, logger) -> List[str]:
    """Extract grid variables from the specified URL."""
    response = requests.get(file_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    target_table = None
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if first_row and "Grid Variables" in first_row.get_text():
            target_table = table
            break

    if not target_table:
        return []

    values = []
    for row in target_table.find_all("tr")[1:]:
        td = row.find("td")
        input_tag = td.find("input")
        if input_tag and "value" in input_tag.attrs:
            values.append(input_tag["value"])

    logger.debug(values)
    return values

def download(url: str, file_path: str, logger: logging.Logger) -> None:
    """Download a file from the specified URL and save it to the file path."""
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, "wb") as file:
        file.write(response.content)
    logger.debug(f"Saved data to {file_path}")

if __name__ == "__main__":
    main()
