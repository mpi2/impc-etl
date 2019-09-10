from impc_etl.shared.transformations.experiments import *
from impc_etl.jobs.extract.dcc_extractor import *
from impc_etl.jobs.clean.experiment_cleaner import *
from impc_etl.jobs.extract.impress_extractor import extract_impress
import os
import pytest

FIXTURES_PATH = (
    os.environ["FIXTURES_PATH"]
    if os.environ["FIXTURES_PATH"] is not None
    else "tests/data/fixtures/"
)
INPUT_PATH = (
    os.environ["INPUT_PATH"]
    if os.environ["INPUT_PATH"] is not None
    else "tests/data/xml/"
)


@pytest.fixture(scope="session")
def experiment_df(spark_session):
    if os.path.exists(FIXTURES_PATH + "experiment_parquet"):
        experiment_df = spark_session.read.parquet(FIXTURES_PATH + "experiment_parquet")
    else:
        dcc_df = extract_dcc_xml_files(spark_session, INPUT_PATH, "experiment")
        experiment_df = get_experiments_by_type(dcc_df, "experiment")
        experiment_df = clean_experiments(experiment_df)
        experiment_df.write.mode("overwrite").parquet(
            FIXTURES_PATH + "experiment_parquet"
        )
    return experiment_df


@pytest.fixture(scope="session")
def mouse_df(spark_session):
    if os.path.exists(FIXTURES_PATH + "mouse_normalized_parquet"):
        mouse_df = spark_session.read.parquet(
            FIXTURES_PATH + "mouse_normalized_parquet"
        )
    else:
        dcc_df = extract_dcc_xml_files(spark_session, INPUT_PATH, "specimen")
        mouse_df = get_specimens_by_type(dcc_df, "mouse")
        mouse_df.write.mode("overwrite").parquet(
            FIXTURES_PATH + "mouse_normalized_parquet"
        )
    return mouse_df


@pytest.fixture(scope="session")
def embryo_df(spark_session):
    if os.path.exists(FIXTURES_PATH + "embryo_normalized_parquet"):
        embryo_df = spark_session.read.parquet(
            FIXTURES_PATH + "embryo_normalized_parquet"
        )
    else:
        dcc_df = extract_dcc_xml_files(spark_session, INPUT_PATH, "specimen")
        embryo_df = get_specimens_by_type(dcc_df, "embryo")
        embryo_df.write.mode("overwrite").parquet(
            FIXTURES_PATH + "embryo_normalized_parquet"
        )
    return embryo_df


@pytest.fixture(scope="session")
def pipeline_df(spark_session):
    if os.path.exists(FIXTURES_PATH + "pipeline_parquet"):
        pipeline_df = spark_session.read.parquet(FIXTURES_PATH + "pipeline_parquet")
    else:
        pipeline_df = extract_impress(
            spark_session, "https://api.mousephenotype.org/impress/", "pipeline"
        )
        pipeline_df.write.mode("overwrite").parquet(FIXTURES_PATH + "pipeline_parquet")
    return pipeline_df


class TestExperimentNormalizer:
    def test_generate_metadata_group(
        self, experiment_df, mouse_df, embryo_df, pipeline_df
    ):
        specimen_cols = [
            "_centreID",
            "_specimenID",
            "_colonyID",
            "_isBaseline",
            "_productionCentre",
            "_phenotypingCentre",
            "phenotyping_consortium",
        ]

        mouse_specimen_df = mouse_df.select(*specimen_cols)
        embryo_specimen_df = embryo_df.select(*specimen_cols)
        specimen_df = mouse_specimen_df.union(embryo_specimen_df)
        experiment_df = experiment_df.alias("experiment")
        specimen_df = specimen_df.alias("specimen")
        experiment_specimen_df = experiment_df.join(
            specimen_df,
            (experiment_df["_centreID"] == specimen_df["_centreID"])
            & (experiment_df["specimenID"] == specimen_df["_specimenID"]),
        ).limit(10)
        experiment_specimen_df = generate_metadata_group(
            experiment_specimen_df, pipeline_df
        )
        experiment_specimen_df.show(vertical=True, truncate=False)
        assert True
