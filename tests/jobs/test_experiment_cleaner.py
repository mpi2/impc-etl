from impc_etl.shared.transformations.experiments import *
from impc_etl.jobs.extract.dcc_extractor import *
import os
import pytest

FIXTURES_PATH = '../data/fixtures/'
INPUT_PATH = '../data/xml/'


@pytest.fixture(scope="session")
def experiment_df(spark_session):
    if os.path.exists(FIXTURES_PATH + 'experiment_raw_parquet'):
        experiment_df = spark_session.read.parquet(FIXTURES_PATH + 'experiment_raw_parquet')
    else:
        dcc_df = extract_dcc_xml_files(spark_session, INPUT_PATH, 'experiment')
        experiment_df = get_experiments_by_type(dcc_df, 'experiment')
        experiment_df.write.mode('overwrite').parquet(FIXTURES_PATH + 'experiment_raw_parquet')
    return experiment_df


class TestExperimentCleaner:
    """
    After mapping centre id, all the rows should contain the mapped ID using predefined CENTRE ID MAP
    After mapping project id, all the rows should contain the mapped ID using predefined PROJECT ID MAP
    After standarize europhenome experiments, all the EuroPhenome experiments
          should have the specimen id trucanted
    After drop skipped experiments, there should not be not
          3i experiments with the predefined experiment ids
    After standarize_3i_experiments, all the 3i experiments that don't have
          a valid project id should have MGP instead
    """

    def test_map_centre_id(self, experiment_df):
        centre_ids = experiment_df.select('_centreID').distinct().collect()
        centre_ids = [row['_centreID'] for row in centre_ids]
        experiment_mapped_df = experiment_df.transform(map_centre_id)
        for centre_id in centre_ids:
            df_diff = experiment_df.where(col('_centreID') == centre_id).drop('_centreID') \
                .subtract(
                experiment_mapped_df.where(
                    col('_centreID') == Constants.CENTRE_ID_MAP[centre_id.lower()]).drop('_centreID')
            )
            assert df_diff.count() == 0

    def test_map_project_id(self, experiment_df):
        project_ids = experiment_df.select('_project').distinct().collect()
        project_ids = [row['_project'] for row in project_ids]
        experiment_mapped_df = experiment_df.transform(map_project_id)
        for project_id in project_ids:
            df_diff = experiment_df.where(col('_project') == project_id).drop('_project') \
                .subtract(
                experiment_mapped_df.where(
                    col('_project') == Constants.PROJECT_ID_MAP[project_id.lower()]).drop('_project')
            )
            assert df_diff.count() == 0

    def test_standarize_europhenome_experiments(self, experiment_df):
        #TODO fix unit test to match truncate logic
        experiment_df = experiment_df.transform(standarize_europhenome_experiments)
        experiment_df = experiment_df\
            .where(col('_dataSource') == 'EuroPhenome')\
            .where(col('specimenID').like('%_%'))
        assert experiment_df.count() == 0

    def test_drop_skipped_experiments(self, experiment_df):
        experiment_df = experiment_df.transform(drop_skipped_experiments)
        experiment_df = experiment_df\
            .where(col('_centreID') == 'Ucd')\
            .where(col('_experimentID').isin({'GRS_2013-10-09_4326', 'GRS_2014-07-16_8800'}))
        assert experiment_df.count() == 0

    def test_drop_skipped_procedures(self, experiment_df):
        experiment_df = experiment_df.transform(drop_skipped_procedures)
        pattern = '|'.join([p_id + '.+' for p_id in Constants.SKIPPED_PROCEDURES])
        experiment_df = experiment_df\
            .where((col('_procedureID').rlike(pattern)) & (col('_dataSource') != '3i'))
        assert experiment_df.count() == 0

    def test_standarize_3i_experiments(self, experiment_df):
        experiment_df = experiment_df.transform(standarize_3i_experiments)
        assert True

