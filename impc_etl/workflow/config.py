import luigi
from luigi.contrib.hdfs import HdfsTarget


class ImpcConfig(luigi.Config):
    dcc_xml_path = luigi.Parameter(default="./")
    output_path = luigi.Parameter(default="./")
    deploy_mode = luigi.Parameter(default="cluster")

    def get_target(self, path):
        return (
            luigi.LocalTarget(path) if self.deploy_mode == "local" else HdfsTarget(path)
        )
