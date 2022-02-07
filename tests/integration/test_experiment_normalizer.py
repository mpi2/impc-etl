import os

import pytest
from impc_etl.transformations.experiment_transformations import _get_closest_weight

from impc_etl.jobs.clean.experiment_cleaner import *
from impc_etl.jobs.clean.specimen_cleaner import clean_specimens
from impc_etl.jobs.extract.impress_extractor import extract_impress
from impc_etl.jobs.extract.xml_extraction_helper import *

FIXTURES_PATH = (
    os.environ["FIXTURES_PATH"]
    if "FIXTURES_PATH" in os.environ
    else "tests/data/fixtures/"
)
INPUT_PATH = (
    os.environ["INPUT_PATH"] if "INPUT_PATH" in os.environ else "tests/data/xml/"
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
        mouse_df = clean_specimens(mouse_df)
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
        embryo_df = clean_specimens(embryo_df)
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


@pytest.mark.skip(reason="no way of currently testing this")
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
        ).where(experiment_df["specimenID"].isin(["30216007"]))
        experiment_specimen_df = generate_metadata_group(
            experiment_specimen_df, pipeline_df
        )
        experiment_specimen_df.show(vertical=True, truncate=False)
        assert True

    def test_series_parameter_derivation(
        self, experiment_df, mouse_df, embryo_df, pipeline_df, spark_session
    ):
        experiment_df.where(col("unique_id").isNull()).show(
            vertical=True, truncate=False
        )
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
        )
        experiment_specimen_df = get_derived_parameters(
            spark_session,
            experiment_specimen_df.where(
                (experiment_specimen_df.specimenID == "IM0011_b0047F")
                & (experiment_specimen_df._procedureID == "IMPC_IPG_001")
            ),
            pipeline_df,
        )
        experiment_specimen_df.show(vertical=True, truncate=False)

    def test_provided_derivation(
        self, experiment_df, mouse_df, embryo_df, pipeline_df, spark_session
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
        specimen_df.where(col("_specimenID") == "IM0023_d0089M").show()
        experiment_df = experiment_df.alias("experiment")
        specimen_df = specimen_df.alias("specimen")
        experiment_specimen_df = experiment_df.join(
            specimen_df,
            (experiment_df["_centreID"] == specimen_df["_centreID"])
            & (experiment_df["specimenID"] == specimen_df["_specimenID"]),
        )

        experiment_specimen_df.where(
            experiment_specimen_df.specimenID == "JMC400007078"
        ).where(
            (experiment_specimen_df._procedureID == "IMPC_DXA_001")
            | (experiment_specimen_df._procedureID == "IMPC_OFD_001")
        ).show(
            100, vertical=True, truncate=False
        )

        experiment_specimen_df = get_derived_parameters(
            spark_session,
            experiment_specimen_df.where(
                experiment_specimen_df.specimenID == "JMC400007078"
            ),
            pipeline_df,
        )
        experiment_specimen_df.where(
            (experiment_specimen_df._procedureID == "IMPC_DXA_001")
            | (experiment_specimen_df._procedureID == "IMPC_OFD_001")
        ).show(100, vertical=True, truncate=False)

    def test_retina_combined(
        self, experiment_df, mouse_df, embryo_df, pipeline_df, spark_session
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
        )
        experiment_specimen_df = get_derived_parameters(
            spark_session,
            experiment_specimen_df.where(
                (experiment_specimen_df.specimenID == "IM0023_d0004F")
                & (experiment_specimen_df._procedureID == "IMPC_EYE_001")
            ),
            pipeline_df,
        )
        experiment_specimen_df.show(vertical=True, truncate=False)

    def test_get_closest_weight(
        self, experiment_df, mouse_df, embryo_df, pipeline_df, spark_session
    ):
        specimen_w = [
            dict(
                weightDate="2015-11-20",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="31.7",
                weightDaysOld="111",
            ),
            dict(
                weightDate="2015-09-18",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="23.1",
                weightDaysOld="48",
            ),
            dict(
                weightDate="2015-10-09",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="26.6",
                weightDaysOld="69",
            ),
            dict(
                weightDate="2015-10-06",
                weightParameterID="IMPC_GRS_003_001",
                weightValue="25.7",
                weightDaysOld="66",
            ),
            dict(
                weightDate="2015-09-11",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="21.1",
                weightDaysOld="41",
            ),
            dict(
                weightDate="2015-09-04",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="18.8",
                weightDaysOld="34",
            ),
            dict(
                weightDate="2015-10-23",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="25.4",
                weightDaysOld="83",
            ),
            dict(
                weightDate="2015-08-28",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="16.5",
                weightDaysOld="27",
            ),
            dict(
                weightDate="2015-11-06",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="27.7",
                weightDaysOld="97",
            ),
            dict(
                weightDate="2015-10-02",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="24.7",
                weightDaysOld="62",
            ),
            dict(
                weightDate="2015-09-25",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="23.5",
                weightDaysOld="55",
            ),
            dict(
                weightDate="2015-11-11",
                weightParameterID="IMPC_DXA_001_001",
                weightValue="29.3",
                weightDaysOld="102",
            ),
            dict(
                weightDate="2015-10-16",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="28",
                weightDaysOld="76",
            ),
            dict(
                weightDate="2015-11-13",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="28.2",
                weightDaysOld="104",
            ),
            dict(
                weightDate="2015-10-30",
                weightParameterID="IMPC_BWT_001_001",
                weightValue="28.6",
                weightDaysOld="90",
            ),
        ]
        experiment_date = "2015-10-30"
        procedure_group = "IMPC_BWT"

        print(_get_closest_weight(experiment_date, procedure_group, specimen_w))

    def test_body_weight_calc(
        self, experiment_df, mouse_df, embryo_df, pipeline_df, spark_session
    ):
        experiment_df = get_associated_body_weight(experiment_df, mouse_df)
        experiment_df.where((experiment_df.specimenID == "IM0025_e0098M")).show(
            vertical=True, truncate=False
        )

    def test_generate_age_information(self, experiment_df, mouse_df, spark_session):
        experiment_df = generate_age_information(experiment_df, mouse_df)
        experiment_df.where(col("specimenID") == "EADE00012009").show()
        experiment_df.where(col("specimenID") == "IM0011_b0046F").show()
