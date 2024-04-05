from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from luigi import configuration


class SmallPySparkTask(PySparkTask):
    @property
    def conf(self):
        return self._dict_config(
            "spark.cores.max=48"
            + " | "
            + configuration.get_config().get(self.spark_version, "conf", None)
        )
