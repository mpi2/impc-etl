import luigi
from luigi.contrib.hdfs import HdfsTarget


class ImpcConfig(luigi.Config):
    dcc_xml_path = luigi.Parameter(default="./")
    output_path = luigi.Parameter(default="./")
    deploy_mode = luigi.Parameter(default="client")

    def get_target(self, path):
        return (
            luigi.LocalTarget(path) if self.deploy_mode in ["local", "client"] else HdfsTarget(path)
        )
