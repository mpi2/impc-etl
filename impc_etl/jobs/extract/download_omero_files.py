import os
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import click
import urllib.parse
from urllib.parse import unquote


def query_solr(solr_url, pipeline_stable_id, procedure_stable_id):
    """
    Query a Solr instance for documents matching exact fields.

    Args:
        solr_url (str): Base URL of the Solr instance.
        pipeline_stable_id (str): Value to match for the pipeline_stable_id field.
        procedure_stable_id (str): Value to match for the procedure_stable_id field.

    Returns:
        list: List of matching documents as JSON objects.
    """
    try:
        # Build the query parameters
        query_params = {
            "q": f'pipeline_stable_id:"{pipeline_stable_id}" AND procedure_stable_id:"{procedure_stable_id}"',
            "wt": "json",
            "rows": 200000,  # Adjust as needed
        }
        query_string = urllib.parse.urlencode(query_params)
        full_url = f"{solr_url}/select?{query_string}"

        # Perform the query
        response = requests.get(full_url)
        response.raise_for_status()

        # Parse the response
        data = response.json()
        return data.get("response", {}).get("docs", [])

    except requests.RequestException as e:
        print(f"Failed to query Solr: {e}")
        return []


def download_file(item, base_dir):
    """
    Download a single file and save it in the structured directory.

    Args:
        item (dict): JSON object containing file metadata.
        base_dir (str): Base directory to save the files.

    Returns:
        str: Status message about the download.
    """
    try:
        # Extract required fields
        download_url = item.get("download_url")
        phenotyping_center = item.get("phenotyping_center", "unknown_center")
        pipeline_stable_id = item.get("pipeline_stable_id", "unknown_pipeline")
        procedure_stable_id = item.get("procedure_stable_id", "unknown_procedure")
        parameter_stable_id = item.get("parameter_stable_id", "unknown_parameter")
        sample_group = item.get("biological_sample_group", "unknown_parameter")
        observation_id = item.get("observation_id", "unknown_observation")

        # Ensure the download URL is valid
        if not download_url:
            return f"Missing download URL for image ID: {item.get('observation_id', 'unknown_id')}"

        # Construct the full download URL if it is relative
        if download_url.startswith("//"):
            download_url = "https:" + download_url.replace(
                "www.ebi.ac.uk/mi/media/omero", "wwwdev.ebi.ac.uk/mi/media/omero"
            )

        # Download the file to determine its extension
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        # Attempt to extract the filename from the Content-Disposition header
        content_disposition = response.headers.get("Content-Disposition", "")
        if "filename=" in content_disposition:
            filename = content_disposition.split("filename=")[1].strip('"')
            extension = os.path.splitext(filename)[1]
        else:
            # Fallback: Use MIME type to determine extension
            content_type = response.headers.get(
                "Content-Type", "application/octet-stream"
            )
            extension = (
                "." + content_type.split("/")[-1] if "/" in content_type else ".bin"
            )

        # Construct the target file path
        target_dir = os.path.join(
            base_dir,
            phenotyping_center,
            pipeline_stable_id,
            procedure_stable_id,
            parameter_stable_id,
            sample_group,
        )
        os.makedirs(target_dir, exist_ok=True)
        target_file_path = os.path.join(target_dir, f"{observation_id}{extension}")

        # Save the file to the target path
        with open(target_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return f"File saved: {target_file_path}"

    except requests.RequestException as e:
        return f"Failed to download file for image ID: {item.get('observation_id', 'unknown_id')}. Error: {e}"
    except Exception as e:
        return f"Unexpected error for image ID: {item.get('observation_id', 'unknown_id')}. Error: {e}"


def download_and_save(json_list, base_dir, log_failed, log_success):
    """
    Download files from a list of JSON objects in parallel and save them in a structured directory.

    Args:
        json_list (list): List of JSON objects containing file metadata.
        base_dir (str): Base directory to save the files.
        log_file (str): Path to the log file for failed downloads.
    """
    max_workers = 10
    failed_downloads = []
    success_downloads = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_file, item, base_dir) for item in json_list]

        for future in as_completed(futures):
            result = future.result()
            print(result)
            if result.startswith("Failed"):
                failed_downloads.append(result)
            else:
                success_downloads.append(result)

    # Log failed downloads
    if failed_downloads:
        with open(log_failed, "w") as log:
            log.write("\n".join(failed_downloads))
        print(f"Failed downloads logged in {log_failed}")

        # Log failed downloads
    if success_downloads:
        with open(log_success, "w") as log:
            log.write("\n".join(success_downloads))
        print(f"Successful downloads logged in {log_success}")


@click.command()
@click.argument("solr_url")
@click.argument("pipeline_stable_id")
@click.argument("procedure_stable_id")
@click.argument("base_directory", type=click.Path())
@click.option(
    "--log-failed",
    default="failed_downloads.log",
    type=click.Path(),
    help="Path to the log file for failed downloads.",
)
@click.option(
    "--log-success",
    default="successful_downloads.log",
    type=click.Path(),
    help="Path to the log file for failed downloads.",
)
def main(
    solr_url,
    pipeline_stable_id,
    procedure_stable_id,
    base_directory,
    log_failed,
    log_success,
):
    """
    CLI tool to query Solr for JSON documents and download files based on the results.

    SOLR_URL: Base URL of the Solr instance.
    PIPELINE_STABLE_ID: Exact match value for the pipeline_stable_id field.
    PROCEDURE_STABLE_ID: Exact match value for the procedure_stable_id field.
    BASE_DIRECTORY: Base directory to save the downloaded files.
    """
    # Query Solr
    json_list = query_solr(solr_url, pipeline_stable_id, procedure_stable_id)

    # Download and save files
    download_and_save(json_list, base_directory, log_failed, log_success)


if __name__ == "__main__":
    main()
