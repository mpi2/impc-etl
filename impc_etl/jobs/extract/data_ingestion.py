"""
    This module groups together tasks to extract and organize raw data from different data sources and
    groups it together on the Data Release directory.
"""

import os
import shutil
from pathlib import Path

import requests
from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import task
from airflow.utils.dates import days_ago

# DAG definition
with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Data Release / Data Ingestion",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "data-release"],
    params={
        "data-release-tag": Param(
            "dr23.0", type="string", description="Data release tag"
        ),
    },
) as dag:
    # Access DAG parameters
    dr_tag = dag.params["data-release-tag"]
    base_path = Variable.get("data_release_work_dir")
    input_data_path = f"{base_path}/{dr_tag}"
    data_archive_path = Variable.get("data_archive_path")

    # Define datasets using dynamic URI

    # Function to create directories dynamically
    @task(
        task_id="create_directories",
        task_display_name="Create input data directories.",
        outlets=[Dataset(f"file://{input_data_path}")],
    )
    def create_directories():
        input_paths = [
            "raw-data",
            "tracking",
            "mgi",
            "ontologies",
            "output",
            "solr",
            "misc",
            "xml",
            "xml/impc",
            "xml/3i",
            "xml/europhenome",
            "xml/pwg",
        ]
        for path in input_paths:
            os.makedirs(f"{input_data_path}/{path}", exist_ok=True)
        return input_data_path

    # Function to fetch tracking data dynamically
    @task(
        task_id="fetch_gene_interest_report",
        task_display_name="Fetch gene interest report data from GenTar.",
        inlets=[Dataset(f"file://{input_data_path}")],
        outlets=[Dataset(f"file://{input_data_path}/tracking/gene_interest.tsv")],
    )
    def fetch_gene_interest_report(output_path: str):
        url = "https://www.gentar.org/tracker-api/api/reports/gene_interest"
        response = requests.get(url)
        with open(f"{output_path}/tracking/gene_interest.tsv", "w") as f:
            f.write(response.text)
        return f"{output_path}/tracking/gene_interest.tsv"

    @task(
        task_id="fetch_tracking_data",
        task_display_name="Fetch tracking data from GenTar.",
        inlets=[Dataset(f"file://{input_data_path}")],
        outlets=[
            Dataset(f"file://{input_data_path}/tracking/gene_interest.tsv"),
            Dataset(f"file://{input_data_path}/tracking/phenotyping_colonies.tsv"),
            Dataset(f"file://{input_data_path}/tracking/gentar-products.tsv"),
        ],
    )
    def fetch_tracking_data(output_path: str):
        # Fetch the gene interest and phenotyping colonies reports from GenTar API
        report_names = ["gene_interest", "phenotyping_colonies"]
        for report in report_names:
            url = f"https://www.gentar.org/tracker-api/api/reports/{report}"
            response = requests.get(url)
            with open(f"{output_path}/tracking/{report}.tsv", "w") as f:
                f.write(response.text)
        # Copy GenTar Products Report from the data archive path to the output path using shutil
        shutil.copy(
            f"{data_archive_path}/gentar-data-archive/product_reports/gentar-products-latest.tsv",
            f"{output_path}/tracking/gentar-products.tsv",
        )
        return (
            f"{output_path}/tracking/gene_interest.tsv",
            f"{output_path}/tracking/phenotyping_colonies.tsv",
            f"{output_path}/tracking/gentar-products.tsv",
        )

    @task(
        task_id="fetch_mgi_data",
        task_display_name="Fetch MGI data.",
        inlets=[Dataset(f"file://{input_data_path}")],
        outlets=[
            Dataset(f"file://{input_data_path}/mgi/MGI_Strain.rpt"),
            Dataset(f"file://{input_data_path}/mgi/MGI_PhenotypicAllele.rpt"),
            Dataset(f"file://{input_data_path}/mgi/MRK_List1.rpt"),
            Dataset(f"file://{input_data_path}/mgi/HGNC_AllianceHomology.rpt"),
            Dataset(f"file://{input_data_path}/mgi/MGI_GenePheno.rpt"),
        ],
    )
    def fetch_mgi_data(output_path):
        report_names = [
            "MGI_Strain.rpt",
            "MGI_PhenotypicAllele.rpt",
            "MRK_List1.rpt",
            "HGNC_AllianceHomology.rpt",
            "MGI_GenePheno.rpt",
        ]
        for report_name in report_names:
            response = requests.get(
                f"https://www.informatics.jax.org/downloads/reports/{report_name}"
            )
            with open(f"{output_path}/mgi/{report_name}", "w") as f:
                f.write(response.text)
        return (
            f"{output_path}/mgi/MGI_Strain.rpt",
            f"{output_path}/mgi/MGI_PhenotypicAllele.rpt",
            f"{output_path}/mgi/MRK_List1.rpt",
            f"{output_path}/mgi/HGNC_AllianceHomology.rpt",
            f"{output_path}/mgi/MGI_GenePheno.rpt",
        )

    # Define the DAG structure
    path = create_directories()
    fetch_tracking_data(path)
    fetch_mgi_data(path)
