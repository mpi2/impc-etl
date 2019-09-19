import sys
import logging
from typing import Callable
from pyspark.sql.dataframe import DataFrame
import os


class YarnLogger:
    @staticmethod
    def setup_logger():
        if "LOG_DIRS" not in os.environ:
            sys.stderr.write(
                "Missing LOG_DIRS environment variable, pyspark logging disabled"
            )
            return

        file = os.environ["LOG_DIRS"].split(",")[0] + "/pyspark.log"
        logging.basicConfig(
            filename=file,
            level=logging.INFO,
            format="%(asctime)s %(levelname)-4s %(filename)s:%(funcName)s():%(lineno)s: %(message)4s",
        )

    def __getattr__(self, key):
        return getattr(logging, key)


YarnLogger.setup_logger()
logger = YarnLogger()


def transform(self: DataFrame, f: Callable) -> DataFrame:
    return f(self)


DataFrame.transform = transform
