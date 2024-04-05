from luigi.contrib.spark import SparkSubmitTask, PySparkTask
from luigi import configuration


class SmallPySparkTask(PySparkTask):
    @property
    def total_executor_cores(self):
        return 48
