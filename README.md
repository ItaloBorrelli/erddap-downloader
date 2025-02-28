# ERDDAP Dataset Downloader

This script allows you to download datasets from multiple ERDDAP servers in various formats. It handles multiple ERDDAP URLs, specified dataset IDs, and different data formats. The script also logs any missed formats and saves them to a file. It only downloads table and not grid structured data at the moment.

## Features

- Supports multiple ERDDAP URLs.
- Allows specifying dataset IDs for a single ERDDAP URL.
- Downloads datasets in various formats (e.g. `nc`, `das`, `iso19115`).
- Logs missed formats and saves them to a file.
- Organizes downloaded files in a structured directory based on the ERDDAP URL and dataset ID.
- Allows specifying a custom downloads folder.

## Installation

To use this script, you need to have Python installed on your system. You can install the required dependencies using `pip`.

```sh
pip install -r requirements.txt
```

## Usage

### Arguments

- `--erddap-urls`: Comma-separated list of ERDDAP URLs. (required)
- `--formats`: Comma-separated list of formats to download. Default is nc,das,iso19115.
- `--datasetIDs`: Comma-separated list of dataset IDs to download. Can only be specified if there is one ERDDAP URL.
- `--downloads-folder`: Folder to save the downloaded files. Default is downloads.

### Examples

Specify multiple ERDDAP URLs and fetch datasets:

```bash
python downloader.py --erddap-urls https://example1.com/erddap,https://example2.com/erddap --formats nc,das,csv,tsv,json
```

Specify dataset IDs for a single ERDDAP URL:

```bash
python downloader.py --erddap-urls https://example.com/erddap --datasetIDs dataset1,dataset2,dataset3 --formats nc,das,csv,tsv,json
```

Specify a custom downloads folder:

```bash
python downloader.py --erddap-urls https://example.com/erddap --datasetIDs dataset1,dataset2,dataset3 --formats nc,das,csv,tsv,json --downloads-folder /path/to/custom/downloads
```

### Directory Structure

The downloaded files are organized in the following directory structure:

```txt
{downloads_folder}/{erddap_url_no_protocol_no_path}/{dataset_id}/{dataset_id}.{fmt}
```

- `{downloads_folder}`: The folder specified by the --downloads-folder argument.
- `{erddap_url_no_protocol_no_path}`: The domain name of the ERDDAP URL (e.g., dap.onc.uvic.ca).
- `{dataset_id}`: The ID of the dataset.
- `{fmt}`: The format of the downloaded file (e.g., nc, das, csv).

### Missed Formats Logging

If any formats fail to download, the script will log the missed formats to a file named missed_formats.txt within the specified downloads folder. The file will contain the ERDDAP URL, dataset ID, and format that failed to download.
