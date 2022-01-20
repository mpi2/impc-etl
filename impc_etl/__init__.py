"""

IMPC ETL Module, groups together all the helper functions, config files, jobs and workflow logic
to process the data for power up https://mousephenotype.org/data.

"""
import sys
import logging
from typing import Callable
from pyspark.sql.dataframe import DataFrame

# Setting up logger object for the whole module
logger = logging.getLogger(__name__)

# Trying to set up the right output so Spark can gather logs from the specific tasks
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)-4s %(filename)s:%(funcName)s():%(lineno)s: %(message)4s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def transform(self: DataFrame, f: Callable) -> DataFrame:
    """
    Transform function to be added to the PySpark DataFrame class
    so it's possible to chain several transformation calls:

    - Using this:
    ```
    df.transform(my_function)
      .transform(my_other_function)
    ```

    - Instead of this:
    ```
    df = my_function(df)
    df = my_other_function(df)
    ```

    This makes easier to use functions that take a DataFrame as an input and return a DataFrame.

    Parameters
    ----------
    self: The DataFrame object invoking the transformation
    f: The function to be applied to the DataFrame
    Returns
    -------
    The input DataFrame (self) transformed by applying he Functions (f)
    """
    return f(self)


DataFrame.transform = transform
