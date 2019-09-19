import luigi
from luigi.contrib.hdfs import HdfsTarget


class ImpcConfig(luigi.Config):
    xml_path = luigi.Parameter(default="./")
    output_path = luigi.Parameter(default="./")
    deploy_mode = luigi.Parameter(default="local")

    def get_target(self, path):
        return (
            luigi.LocalTarget(path) if self.deploy_mode == "local" else HdfsTarget(path)
        )
