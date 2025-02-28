import argparse
import requests
import csv
import re
import os
from io import StringIO
from urllib.parse import quote, urlparse

def main():
    parser = argparse.ArgumentParser(description="Download ERDDAP datasets.")
    parser.add_argument("--erddap-urls", type=str, required=True, help="Comma-separated list of ERDDAP URLs.")
    parser.add_argument("--formats", type=str, default="nc,das,iso19115", help="Download these formats for every dataset.")
    parser.add_argument("--datasetIDs", type=str, help="Comma-separated list of dataset IDs to download. Can only be specified if there is one ERDDAP URL.")
    parser.add_argument("--downloads-folder", type=str, default="downloads", help="Folder to save the downloaded files.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading files that already exist in the download folder.")
    args = parser.parse_args()

    erddap_urls = args.erddap_urls.split(',')
    formats = args.formats.split(',')
    missed_formats = []

    if args.datasetIDs and len(erddap_urls) > 1:
        print("Error: datasetIDs can only be specified if there is one ERDDAP URL.")
        return

    for erddap_url in erddap_urls:
        print(f"Processing ERDDAP URL: {erddap_url}")
        erddap_url_no_protocol_no_path = urlparse(erddap_url).netloc

        dataset_ids = get_dataset_ids(erddap_url, args.datasetIDs, len(erddap_urls))

        for dataset_id in dataset_ids:
            download_dir = os.path.join(args.downloads_folder, erddap_url_no_protocol_no_path, dataset_id)
            os.makedirs(download_dir, exist_ok=True)

            if args.skip_existing and all_formats_exist(download_dir, dataset_id, formats):
                print(f"All formats for datasetID {dataset_id} already exist. Skipping.")
                continue

            variables = fetch_variables(erddap_url, dataset_id)
            missed_formats.extend(download_dataset_files(erddap_url, dataset_id, variables, formats, download_dir, args.skip_existing))

    report_missed_formats(missed_formats, args.downloads_folder)

def get_dataset_ids(erddap_url, specified_dataset_ids, num_urls):
    if specified_dataset_ids and num_urls == 1:
        return specified_dataset_ids.split(',')

    datasets_url = f"{erddap_url}/tabledap/allDatasets.csv?datasetID%2CdataStructure"
    response = requests.get(datasets_url)
    response.raise_for_status()

    dataset_ids = []
    csv_data = StringIO(response.text)
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        if row and row[0] and row[1] == 'table':
            dataset_ids.append(row[0])
    print("Fetched Dataset IDs:", dataset_ids)
    return dataset_ids

def all_formats_exist(download_dir, dataset_id, formats):
    return all(
        os.path.exists(os.path.join(download_dir, f"{dataset_id}.{fmt}")) for fmt in formats
    )

def fetch_variables(erddap_url, dataset_id):
    dds_url = f"{erddap_url}/tabledap/{dataset_id}.dds"
    response = requests.get(dds_url)
    response.raise_for_status()
    print(f"Fetched .dds file for datasetID: {dataset_id}")
    return extract_variables(response.text)

def extract_variables(dds_content):
    variable_pattern = re.compile(r'\b(Byte|Int32|UInt32|Float64|String|Url)\s+(\w+);', re.MULTILINE)
    variables = [match[1] for match in variable_pattern.findall(dds_content)]
    return variables

def download_dataset_files(erddap_url, dataset_id, variables, formats, download_dir, skip_existing):
    missed_formats = []
    for fmt in formats:
        file_path = os.path.join(download_dir, f"{dataset_id}.{fmt}")
        if skip_existing and os.path.exists(file_path):
            print(f"Skipping existing file: {file_path}")
            continue

        url = build_dataset_url(erddap_url, dataset_id, fmt, variables)
        try:
            response = requests.get(url)
            response.raise_for_status()
            print(f"Fetched data for datasetID {dataset_id} in format {fmt}")

            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Saved data to {file_path}")
        except requests.exceptions.HTTPError as e:
            print(f"Failed to fetch data for datasetID {dataset_id} in format {fmt}: {e}")
            missed_formats.append((erddap_url, dataset_id, fmt))
    return missed_formats

def build_dataset_url(erddap_url, dataset_id, fmt, variables):
    query_string = quote(','.join(variables))
    return f"{erddap_url}/tabledap/{dataset_id}.{fmt}?{query_string}"

def report_missed_formats(missed_formats, downloads_folder):
    if missed_formats:
        print("\nMissed formats:")
        for erddap_url, dataset_id, fmt in missed_formats:
            print(f"ERDDAP URL: {erddap_url}, Dataset ID: {dataset_id}, Format: {fmt}")

        missed_formats_file = os.path.join(downloads_folder, "missed_formats.txt")
        with open(missed_formats_file, 'w') as file:
            file.write("Missed formats:\n")
            for erddap_url, dataset_id, fmt in missed_formats:
                file.write(f"ERDDAP URL: {erddap_url}, Dataset ID: {dataset_id}, Format: {fmt}\n")
        print(f"Missed formats have been written to {missed_formats_file}")

if __name__ == "__main__":
    main()
