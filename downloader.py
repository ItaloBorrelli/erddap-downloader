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
    args = parser.parse_args()

    # Split the comma-separated list of ERDDAP URLs
    erddap_urls = args.erddap_urls.split(',')

    # Check if datasetIDs is specified with multiple ERDDAP URLs
    if args.datasetIDs and len(erddap_urls) > 1:
        print("Error: datasetIDs can only be specified if there is one ERDDAP URL.")
        return

    missed_formats = []

    for erddap_url in erddap_urls:
        print(f"Processing ERDDAP URL: {erddap_url}")

        # Strip the protocol and path from the ERDDAP URL for the directory name
        parsed_url = urlparse(erddap_url)
        erddap_url_no_protocol_no_path = parsed_url.netloc

        # If datasetIDs are specified and there is only one ERDDAP URL, use the specified datasetIDs
        if args.datasetIDs and len(erddap_urls) == 1:
            dataset_ids = args.datasetIDs.split(',')
            print("Specified Dataset IDs:", dataset_ids)
        else:
            # Fetch all dataset IDs from the ERDDAP server
            datasets_url = f"{erddap_url}/tabledap/allDatasets.csv?datasetID%2CdataStructure"
            response = requests.get(datasets_url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            dataset_ids = []
            csv_data = StringIO(response.text)
            csv_reader = csv.reader(csv_data)
            next(csv_reader)  # Skip the header row
            for row in csv_reader:
                if row and row[0] and row[1] == 'table':  # Check if the row is not empty, the first column contains a datasetID, and dataStructure is 'table'
                    dataset_ids.append(row[0])
            print("Fetched Dataset IDs:", dataset_ids)

        # Fetch the .dds file for each datasetID and extract variable names
        variables_by_dataset = {}
        for dataset_id in dataset_ids:
            dds_url = f"{erddap_url}/tabledap/{dataset_id}.dds"
            response = requests.get(dds_url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            print(f"Fetched .dds file for datasetID: {dataset_id}")

            # Extract the variable names from the .dds file
            dds_content = response.text
            variables = extract_variables(dds_content)
            variables_by_dataset[dataset_id] = variables

        # Make URL calls for each dataset and format, and save the files
        formats = args.formats.split(',')
        for dataset_id, variables in variables_by_dataset.items():
            for fmt in formats:
                query_string = quote(','.join(variables))
                url = f"{erddap_url}/tabledap/{dataset_id}.{fmt}?{query_string}"

                # Make the URL call
                try:
                    response = requests.get(url)
                    response.raise_for_status()  # Raise an exception for HTTP errors
                    print(f"Fetched data for datasetID {dataset_id} in format {fmt}")

                    # Create the downloads directory if it doesn't exist
                    download_dir = os.path.join(args.downloads_folder, erddap_url_no_protocol_no_path, dataset_id)
                    os.makedirs(download_dir, exist_ok=True)

                    # Save the content to a file
                    file_path = os.path.join(download_dir, f"{dataset_id}.{fmt}")
                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                    print(f"Saved data to {file_path}")
                except requests.exceptions.HTTPError as e:
                    print(f"Failed to fetch data for datasetID {dataset_id} in format {fmt}: {e}")
                    missed_formats.append((erddap_url, dataset_id, fmt))

    # Report missed formats at the end
    if missed_formats:
        print("\nMissed formats:")
        for erddap_url, dataset_id, fmt in missed_formats:
            print(f"ERDDAP URL: {erddap_url}, Dataset ID: {dataset_id}, Format: {fmt}")

        # Write missed formats to a file
        missed_formats_file = os.path.join(args.downloads_folder, "missed_formats.txt")
        with open(missed_formats_file, 'w') as file:
            file.write("Missed formats:\n")
            for erddap_url, dataset_id, fmt in missed_formats:
                file.write(f"ERDDAP URL: {erddap_url}, Dataset ID: {dataset_id}, Format: {fmt}\n")
        print(f"Missed formats have been written to {missed_formats_file}")

def extract_variables(dds_content):
    # Regular expression to match variable declarations within the Sequence
    variable_pattern = re.compile(r'\b(Byte|Int32|UInt32|Float64|String|Url)\s+(\w+);', re.MULTILINE)
    variables = []

    # Find all matches of the variable pattern
    matches = variable_pattern.findall(dds_content)
    for match in matches:
        var_type, var_name = match
        variables.append(var_name)

    return variables

if __name__ == "__main__":
    main()
