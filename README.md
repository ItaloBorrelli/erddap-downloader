# ERDDAP Dataset Downloader

This script allows you to download datasets from multiple ERDDAP servers in various formats. It handles multiple ERDDAP URLs, specified dataset IDs, and different data formats. The script also logs any missed formats and saves them to a file. It only downloads table and not grid structured data at the moment.

## Features

- Supports multiple ERDDAP URLs.
- Allows specifying dataset IDs for a single ERDDAP URL.
- Downloads datasets in various formats, defaulting to `nc`, `das`, `iso19115`.
- Logs missed formats and saves them to a file.
- Organizes downloaded files in a structured directory based on the ERDDAP URL and dataset ID.
- Allows specifying a custom downloads folder.

## Installation

To use this script, you need to have Python installed on your system. You can install the required dependencies using `pip`.

```sh
pip install requests
