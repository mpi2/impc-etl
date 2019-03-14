import sys
import logging
from typing import Callable
from pyspark.sql.dataframe import DataFrame


logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s %(levelname)-4s %(filename)s:%(funcName)s():%(lineno)s: %(message)4s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def transform(self: DataFrame, f: Callable) -> DataFrame:
    return f(self)


DataFrame.transform = transform
