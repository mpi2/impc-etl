import luigi
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.s3 import S3Target


class ImpcConfig(luigi.Config):
    dcc_xml_path = luigi.Parameter(default="./")
    output_path = luigi.Parameter(default="./")
    deploy_mode = luigi.Parameter(default="client")
    release_tag = luigi.Parameter(default="dr19.1")
    s3_bucket = luigi.Parameter(default="etl-work")

    def get_target(self, path):
        if self.deploy_mode in ["local", "client"]:
            return luigi.LocalTarget(path)

        if self.deploy_mode == "aws":
            return S3Target(f"s3://{self.s3_bucket}/{self.release_tag}/output/{path}")

        return (
            luigi.LocalTarget(path)
            if self.deploy_mode in ["local", "client"]
            else HdfsTarget(path)
        )
