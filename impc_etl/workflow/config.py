import luigi


class ImpcConfig(luigi.Config):
    xml_path = luigi.Parameter(default="./")
    output_path = luigi.Parameter(default="./")
