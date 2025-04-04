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
        "--formats",
        type=str,
        default="ncCF,das",
        help="Download these formats for every dataset.",
    )
    parser.add_argument(
        "--datasetIDs",
        type=str,
        help="Comma-separated list of dataset IDs to download.",
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
        help="Skip downloading files that already exist.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )
    parser.add_argument(
        "--grid-datasets-1",
        "-g",
        action="store_true",
        help="Include grid datasets with files.",
    )
    parser.add_argument(
        "--grid-datasets-2",
        "-j",
        action="store_true",
        help="Include grid datasets without files.",
    )
    parser.add_argument(
        "--table-datasets", "-t", action="store_true", help="Include table datasets."
    )

    args = parser.parse_args()

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

    if (
        not args.table_datasets
        and not args.grid_datasets_1
        and not args.grid_datasets_2
    ):
        logger.error(
            "At least one of --table-datasets (-t), --grid-datasets-1 (-g), or --grid-datasets-2 (-j) must be selected."
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
                download_dir, dataset_id, formats
            ):
                logger.debug(
                    f"All formats for datasetID {dataset_id} already exist. Skipping."
                )
                continue

            is_table = data_structure == "table"
            has_files = bool(file_url)
            did_download = False

            if is_table and args.table_datasets:
                did_download = True
                if has_files:
                    error_report.extend(
                        download_files(
                            erddap_url, dataset_id, file_url, download_dir, logger
                        )
                    )
                error_report.extend(
                    download_dataset_files(
                        erddap_url,
                        dataset_id,
                        is_table,
                        formats,
                        download_dir,
                        args.skip_existing,
                        logger,
                        [],
                    )
                )

            elif not is_table and has_files and args.grid_datasets_1:
                did_download = True
                error_report.extend(
                    download_files(
                        erddap_url, dataset_id, file_url, download_dir, logger
                    )
                )

            elif not is_table and not has_files and args.grid_datasets_2:
                did_download = True
                vars = extract_grid_variables_from_url(
                    f"{erddap_url}/griddap/{dataset_id}.html"
                )
                error_report.extend(
                    download_dataset_files(
                        erddap_url,
                        dataset_id,
                        is_table,
                        formats,
                        download_dir,
                        args.skip_existing,
                        logger,
                        vars,
                    )
                )

            if did_download and iso19115_url:
                iso19115_file_path = os.path.join(
                    download_dir, f"{dataset_id}.iso19115"
                )
                download(iso19115_url, iso19115_file_path, logger)

    do_error_report(error_report, args.downloads_folder, start_time, logger)


def download_files(
    erddap_url: str,
    dataset_id: str,
    file_url: str,
    download_dir: str,
    logger: logging.Logger,
) -> List[Tuple[str, str, str, Exception]]:
    """Download files for a dataset from the specified URL."""
    missed_files = []
    try:
        file_names = extract_file_names_from_url(file_url, logger)
    except (RequestException, IncompleteRead) as e:
        logger.error(f"Failed to fetch files for datasetID {dataset_id}. Error: {e}")
        missed_files.append((erddap_url, dataset_id, "all", e))
        return missed_files

    for file_name in file_names:
        url = f"{file_url}{file_name}"
        file_path = os.path.join(download_dir, file_name)
        try:
            download(url, file_path, logger)
        except (RequestException, IncompleteRead) as e:
            logger.error(
                f"Failed to fetch file {file_name} for datasetID {dataset_id}. Error: {e}"
            )
            missed_files.append((erddap_url, dataset_id, file_name, e))
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


def extract_grid_variables_from_url(file_url: str) -> List[str]:
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

    return values


def get_dataset_ids(
    erddap_url: str, specified_dataset_ids: str, logger: logging.Logger
) -> List[Tuple[str, str, str, str]]:
    """Fetch dataset IDs from the ERDDAP server or use specified dataset IDs."""
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


def all_formats_exist(download_dir: str, dataset_id: str, formats: List[str]) -> bool:
    """Check if all required formats for a dataset already exist in the download directory."""
    return all(
        os.path.exists(os.path.join(download_dir, f"{dataset_id}.{fmt}"))
        for fmt in formats
    )


def download_dataset_files(
    erddap_url: str,
    dataset_id: str,
    is_table: bool,
    formats: List[str],
    download_dir: str,
    skip_existing: bool,
    logger: logging.Logger,
    vars: List[str],
) -> List[Tuple[str, str, str, Exception]]:
    """Download dataset files in specified formats."""
    missed_formats = []
    for fmt in formats:
        file_path = os.path.join(download_dir, f"{dataset_id}.{fmt}")
        if skip_existing and os.path.exists(file_path):
            logger.debug(f"Skipping existing file: {file_path}")
            continue
        qs = f"?{'%2C'.join(vars)}" if vars else ""
        path = "tabledap" if is_table else "griddap"
        url = f"{erddap_url}/{path}/{dataset_id}.{fmt}{qs}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, "wb") as file:
                file.write(response.content)
            logger.debug(f"Saved data to {file_path}")
        except HTTPError as e:
            if response.status_code == 400 and fmt == "ncCF":
                if not skip_existing or not os.path.exists(
                    os.path.join(download_dir, f"{dataset_id}.nc")
                ):
                    download_dataset_files(
                        erddap_url,
                        dataset_id,
                        is_table,
                        ["nc"],
                        download_dir,
                        skip_existing,
                        logger,
                        vars,
                    )
                    logger.info(
                        'ncCF not available for cdm_data_type="Other". Downloading nc instead.'
                    )
            else:
                logger.error(
                    f"Failed to fetch data for datasetID {dataset_id} in format {fmt}. Error: {e}"
                )
                missed_formats.append((erddap_url, dataset_id, fmt, e))
        except (RequestException, IncompleteRead) as e:
            logger.error(
                f"Failed to fetch data for datasetID {dataset_id} in format {fmt}. Error: {e}"
            )
            missed_formats.append((erddap_url, dataset_id, fmt, e))
    return missed_formats


def download(url: str, file_path: str, logger: logging.Logger) -> None:
    """Download a file from the specified URL and save it to the file path."""
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, "wb") as file:
        file.write(response.content)
    logger.debug(f"Saved data to {file_path}")


def do_error_report(
    missed_formats: List[Tuple[str, str, str, Exception]],
    downloads_folder: str,
    start_time: datetime,
    logger: logging.Logger,
) -> None:
    """Report missed formats to a CSV file."""
    report_file = os.path.join(
        downloads_folder, f"missed_formats-{datetime.now():%Y-%m-%d_%H:%M:%S}.csv"
    )
    file_exists = os.path.exists(report_file)

    with open(report_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["time", "erddap_url", "datasetID", "missed", "error"])
        for erddap_url, dataset_id, missed, e in missed_formats:
            writer.writerow([start_time.isoformat(), erddap_url, dataset_id, missed, e])

    if missed_formats:
        logger.info(
            f"Some content was missed. Details have been written to {report_file}"
        )


if __name__ == "__main__":
    main()
