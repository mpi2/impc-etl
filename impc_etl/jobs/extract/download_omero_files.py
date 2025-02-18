import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import requests


def query_solr(solr_url, pipeline_stable_id, procedure_stable_id):
    """
    Query a Solr instance for documents matching exact fields.
    """
    try:
        query_params = {
            "q": f'pipeline_stable_id:"{pipeline_stable_id}" AND procedure_stable_id:"{procedure_stable_id}"',
            "wt": "json",
            "rows": 200000,
        }
        query_string = urllib.parse.urlencode(query_params)
        full_url = f"{solr_url}/select?{query_string}"

        response = requests.get(full_url)
        response.raise_for_status()
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
    """Download files from a list of JSON objects in parallel."""
    max_workers = 5
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

    if failed_downloads:
        with open(log_failed, "w") as log:
            log.write("\n".join(failed_downloads))
    if success_downloads:
        with open(log_success, "w") as log:
            log.write("\n".join(success_downloads))


def extract_combinations(solr_url):
    """
    Query Solr to get the list of unique pipeline_stable_id and procedure_stable_id combinations.
    """
    try:
        query_params = {
            "q": "*:*",
            "facet": "on",
            "facet.pivot": "pipeline_stable_id,procedure_stable_id",
            "rows": "0",
            "wt": "json",
        }
        response = requests.get(f"{solr_url}/select", params=query_params)
        response.raise_for_status()

        data = response.json()
        pivot_data = (
            data.get("facet_counts", {})
            .get("facet_pivot", {})
            .get("pipeline_stable_id,procedure_stable_id", [])
        )

        combinations = [
            (item["value"], sub_item["value"])
            for item in pivot_data
            for sub_item in item.get("pivot", [])
        ]
        return combinations
    except requests.RequestException as e:
        print(f"Failed to query Solr: {e}")
        return []


@click.group()
def main():
    pass


@click.command()
@click.argument("solr_url")
@click.argument("output_file", type=click.Path())
def list_combinations(solr_url, output_file):
    """
    List all available pipeline_stable_id and procedure_stable_id combinations from Solr and save to a file.
    """
    combinations = extract_combinations(solr_url)

    with open(output_file, "w") as f:
        for combo in combinations:
            f.write("\t".join(combo) + "\n")

    print(f"Combinations saved to {output_file}")


@click.command()
@click.argument("solr_url")
@click.argument("manifest", type=click.Path(exists=True))
@click.argument("base_dir", type=click.Path())
@click.option(
    "--batch-from", type=int, required=True, help="Start line in the manifest."
)
@click.option("--batch-to", type=int, required=True, help="End line in the manifest.")
@click.option(
    "--log-failed",
    default="failed_downloads.log",
    type=click.Path(),
    help="Log file for failed downloads.",
)
@click.option(
    "--log-success",
    default="successful_downloads.log",
    type=click.Path(),
    help="Log file for successful downloads.",
)
def download_batch(
    solr_url, manifest, base_dir, batch_from, batch_to, log_failed, log_success
):
    """
    CLI tool to query Solr using a manifest file and download images in batches.
    """
    num_lines = sum(1 for _ in open(manifest))

    if batch_to > num_lines:
        batch_to = num_lines

    if batch_from > batch_to:
        print("Invalid batch range.")
        return

    with open(manifest, "r") as f:
        lines = f.readlines()[batch_from:batch_to]

    combinations = [line.strip().split("\t") for line in lines]

    for pipeline_stable_id, procedure_stable_id in combinations:
        json_list = query_solr(solr_url, pipeline_stable_id, procedure_stable_id)
        download_and_save(json_list, base_dir, log_failed, log_success)


main.add_command(list_combinations)
main.add_command(download_batch)

if __name__ == "__main__":
    main()
