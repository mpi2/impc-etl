from impc_etl.jobs.clean.experiment_cleaner import *
from impc_etl.shared import utils
import hashlib
import pytest


class TestExperimentCleaner:
    """
    After mapping centre id, all the rows should contain the mapped ID using predefined CENTRE ID MAP
    After mapping project id, all the rows should contain the mapped ID using predefined PROJECT ID MAP
    After truncate EuroPhenome experiments, all the EuroPhenome experiments
          should have the specimen id truncated
    After drop skipped experiments, there should not be not
          3i experiments with the predefined experiment ids
    After map_3i_experiments, all the 3i experiments that don't have
          a valid project id should have MGP instead
    After dropping if null, the DataFrame should not contain any row with a null value in the specified column
    After generate unique id, the DataFrame should contain a new column with an MD5 hash of the concatenation
          of the unique columns values
    """

    NOT_NULL_COLUMNS = [
        "_centreID",
        "_dataSource",
        "_dateOfExperiment",
        "_pipeline",
        "_project",
        "specimenID",
    ]

    def test_map_centre_ids(self, spark_session):
        experiments = [
            {"_centreID": "gmc", "_experimentID": "0"},
            {"_centreID": "h", "_experimentID": "1"},
            {"_centreID": "ucd", "_experimentID": "2"},
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        centre_ids = experiment_df.select("_centreID").distinct().collect()
        centre_ids = [row["_centreID"] for row in centre_ids]
        experiment_mapped_df = experiment_df.transform(map_centre_ids)
        for centre_id in centre_ids:
            df_diff = (
                experiment_df.where(col("_centreID") == centre_id)
                .drop("_centreID")
                .subtract(
                    experiment_mapped_df.where(
                        col("_centreID") == Constants.CENTRE_ID_MAP[centre_id.lower()]
                    ).drop("_centreID")
                )
            )
            assert df_diff.count() == 0

    def test_map_project_id(self, spark_session):
        experiments = [
            {"_project": "dtcc", "_experimentID": "0"},
            {"_project": "eumodic", "_experimentID": "1"},
            {"_project": "eucomm-eumodic", "_experimentID": "2"},
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)

        project_ids = experiment_df.select("_project").distinct().collect()
        project_ids = [row["_project"] for row in project_ids]
        experiment_mapped_df = experiment_df.transform(map_project_ids)
        for project_id in project_ids:
            df_diff = (
                experiment_df.where(col("_project") == project_id)
                .drop("_project")
                .subtract(
                    experiment_mapped_df.where(
                        col("_project") == Constants.PROJECT_ID_MAP[project_id.lower()]
                    ).drop("_project")
                )
            )
            assert df_diff.count() == 0

    def test_truncate_europhenome_experiments(self, spark_session):
        experiments = [
            {
                "specimenID": "30173140_HMGU",
                "_experimentID": "0",
                "_dataSource": "europhenome",
            },
            {
                "specimenID": "RUSSET/16.2b_4615141_MRC_Harwell",
                "_experimentID": "1",
                "_dataSource": "europhenome",
            },
            {
                "specimenID": "848974_1687897",
                "_experimentID": "2",
                "_dataSource": "impc",
            },
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)

        experiment_df = experiment_df.transform(truncate_europhenome_specimen_ids)

        europhenome_non_mrc = experiment_df.where(col("_experimentID") == "0").first()
        europhenome_mrc = experiment_df.where(col("_experimentID") == "1").first()
        non_europhenome = experiment_df.where(col("_experimentID") == "2").first()

        assert europhenome_non_mrc["specimenID"] == "30173140"
        assert europhenome_mrc["specimenID"] == "RUSSET/16.2b_4615141"
        assert non_europhenome["specimenID"] == "848974_1687897"

    def test_drop_skipped_experiments(self, spark_session):
        experiments = [
            {"_experimentID": "GRS_2013-10-09_4326", "_centreID": "Ucd"},
            {"_experimentID": "GRS_2014-07-16_8800", "_centreID": "Ucd"},
            {"_experimentID": "GRS_2014-07-16_8800", "_centreID": "RBRC"},
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        experiment_df = experiment_df.transform(drop_skipped_experiments)
        experiment_df = experiment_df.where(col("_centreID") == "Ucd").where(
            col("_experimentID").isin({"GRS_2013-10-09_4326", "GRS_2014-07-16_8800"})
        )
        assert experiment_df.count() == 0

    def test_drop_skipped_procedures(self, spark_session):
        experiments = [
            {"_procedureID": "SLM_SLM", "_dataSource": "Ucd"},
            {"_procedureID": "SLM_AGS", "_dataSource": "Ucd"},
            {"_procedureID": "MGP_EEI", "_dataSource": "3i"},
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        experiment_df = experiment_df.transform(drop_skipped_procedures)
        pattern = "|".join([p_id + ".+" for p_id in Constants.SKIPPED_PROCEDURES])
        experiment_df = experiment_df.where(
            (col("_procedureID").rlike(pattern)) & (col("_dataSource") != "3i")
        )
        assert experiment_df.count() == 0

    def test_map_3i_project_ids(self, spark_session):
        experiments = [
            {
                "_project": "INVALID_PROJECT_ID",
                "_dataSource": "3i",
                "_experimentID": "invalid_3i_experiment",
            },
            {
                "_project": "JAX",
                "_dataSource": "3i",
                "_experimentID": "valid_3i_experiment",
            },
            {
                "_project": "MRC",
                "_dataSource": "impc",
                "_experimentID": "valid_non_3i_experiment",
            },
        ]

        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        experiment_df = experiment_df.transform(map_3i_project_ids)

        invalid_3i_experiment = experiment_df.where(
            col("_experimentID") == "invalid_3i_experiment"
        ).first()
        valid_3i_experiment = experiment_df.where(
            col("_experimentID") == "valid_3i_experiment"
        ).first()
        valid_non_3i_experiment = experiment_df.where(
            col("_experimentID") == "valid_non_3i_experiment"
        ).first()

        assert invalid_3i_experiment["_project"] == "MGP"
        assert valid_3i_experiment["_project"] == "JAX"
        assert valid_non_3i_experiment["_project"] == "MRC"

    @pytest.mark.parametrize("column_name", NOT_NULL_COLUMNS)
    def test_drop_if_null(self, spark_session, column_name):
        experiments = []
        for i in range(0, 5):
            experiment = {}
            for not_null_column in self.NOT_NULL_COLUMNS:
                experiment[not_null_column] = (
                    None if i < 2 and not_null_column == column_name else "SOME_VALUE"
                )
            experiments.append(experiment)
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        experiment_df = drop_if_null(experiment_df, column_name)
        assert experiment_df.where(experiment_df[column_name].isNull()).count() == 0

    def test_generate_unique_id(self, spark_session):
        experiments = [
            {
                "_type": "type0",
                "_sourceFile": "file0.xml",
                "_VALUE": "NULL",
                "procedureMetadata": "some value",
                "statusCode": "FAILED",
                "_sequenceID": None,
                "_project": "CHMD",
                "simpleParameter": "dskfdsap",
                "uniqueField0": "A",
                "uniqueField1": "B",
                "uniqueField2": "C",
            },
            {
                "_type": "type0",
                "_sourceFile": "file0.xml",
                "_VALUE": "NULL",
                "procedureMetadata": "some value",
                "statusCode": "FAILED",
                "_sequenceID": "1",
                "_project": "CHMD",
                "simpleParameter": "dsdasda",
                "uniqueField0": "D",
                "uniqueField1": "E",
                "uniqueField2": "F",
            },
        ]
        experiment_df = utils.convert_to_dataframe(spark_session, experiments)
        experiment_df = experiment_df.transform(generate_unique_id)
        processed_experiments = experiment_df.collect()
        assert (
            processed_experiments[0]["unique_id"] == hashlib.md5(b"ABCNA").hexdigest()
        )
        assert processed_experiments[1]["unique_id"] == hashlib.md5(b"DEF1").hexdigest()
