# ERDDAP Dataset Downloader

This script allows you to download datasets from multiple ERDDAP servers in various formats. It handles multiple ERDDAP URLs, specified dataset IDs, and different data formats. The script logs any missed formats and saves them to a CSV file.

## Features

- Supports multiple ERDDAP URLs.
- Allows specifying dataset IDs for a single ERDDAP URL.
- Downloads datasets in various formats (e.g., `nc`, `ncCF`, `das`, `iso19115`).
- Logs missed formats and saves them to a CSV file with detailed information.
- Organizes downloaded files in a structured directory based on the ERDDAP URL and dataset ID.
- Allows specifying a custom downloads folder.
- Implements a retry mechanism for handling transient errors.
- Uses logging with different log levels (DEBUG, INFO, ERROR) to provide detailed output.
- Allows setting the logging level via a command-line argument.

## Installation

To use this script, you need to have Python installed on your system. You can install the required dependencies using `pip`.

```sh
pip install -r requirements.txt
```

## Usage

### Arguments

- `--erddap-urls`: Comma-separated list of ERDDAP URLs. (required)
- `--formats`: Comma-separated list of formats to download. Default is `ncCF,das,iso19115`.
- `--datasetIDs`: Comma-separated list of dataset IDs to download. Can only be specified if there is one ERDDAP URL.
- `--downloads-folder`: Folder to save the downloaded files. Default is `downloads`.
- `--skip-existing`: Skip downloading files that already exist in the download folder.
- `--log-level`: Set the logging level. Options are `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Default is `INFO`.
- One or both of the following needs to be selected:
  - `--grid-datasets` or `-g`: Download grid dataset files.
  - `--table-datasets` or `-t`: Download table dataset files.

### Examples

Specify multiple ERDDAP URLs and fetch datasets:

```sh
python downloader.py --erddap-urls https://example1.com/erddap,https://example2.com/erddap --formats nc,das,csv,tsv,json -t -g
```

Specify dataset IDs for a single ERDDAP URL:

```sh
python downloader.py --erddap-urls https://example.com/erddap --datasetIDs dataset1,dataset2,dataset3 --formats nc,das,csv,tsv,json -t
```

Specify a custom downloads folder:

```sh
python downloader.py --erddap-urls https://example.com/erddap --datasetIDs dataset1,dataset2,dataset3 --formats nc,das,csv,tsv,json -t --downloads-folder /path/to/custom/downloads
```

Set the logging level to `DEBUG`:

```sh
python downloader.py --erddap-urls https://example.com/erddap --formats nc,das -t --log-level DEBUG
```

### Directory Structure

The downloaded files are organized in the following directory structure:

```txt
{downloads_folder}/{erddap_url_no_protocol_no_path}/{dataset_id}/{dataset_id}.{fmt}
```

- `{downloads_folder}`: The folder specified by the `--downloads-folder` argument.
- `{erddap_url_no_protocol_no_path}`: The domain name of the ERDDAP URL (e.g., `dap.onc.uvic.ca`).
- `{dataset_id}`: The ID of the dataset.
- `{fmt}`: The format of the downloaded file (e.g., `nc`, `das`, `csv`).

### Missed Formats Logging

If any formats fail to download, the script will log the missed formats to a CSV file named `missed_formats.csv` within the specified downloads folder. The file will contain the time of the run, ERDDAP URL, dataset ID, and format that failed to download. This allows for easier tracking and analysis of download failures over time.

### Logging

The script uses Python's `logging` module to provide detailed output at different log levels:

- `DEBUG`: Detailed information, useful for diagnosing problems.
- `INFO`: Confirmation that things are working as expected.
- `WARNING`: An indication that something unexpected happened.
- `ERROR`: An error occurred that needs attention.
- `CRITICAL`: A very serious error, indicating that the program itself may be unable to continue running.

You can adjust the logging level using the `--log-level` argument to control the verbosity of the output.
