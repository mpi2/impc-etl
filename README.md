# IMPC ETL Process [![Build Status](https://travis-ci.com/mpi2/impc-etl.svg?branch=master)](https://travis-ci.com/mpi2/impc-etl)
IMPC Extraction, Transformation and Loading process to generate the data that supports [mousephenotype.org](http://mousephenotype.org) among with other internal processes.
## Requirements
- [Python 3+](https://www.python.org/)
- [Spark 2+](https://spark.apache.org/)
- [make](https://www.gnu.org/software/make/)

## How to run it
Download the latest release package from the [releases page](https://github.com/mpi2/impc-etl/releases) and decompress it.
Then submit your job to your Spark 2 cluster using:

```console
spark-submit --py-files impc_etl.zip,libs.zip main.py
```

## Development environment setup
1. Install [Spark 2+](https://spark.apache.org/) and remember to set the ``SPARK_HOME`` environment variable.
2. Fork this repo and then clone your forked version:
    ```console
    git clone https://github.com/USERNAME/impc-etl.git
    cd impc-etl
    ```

3. Run _make_ to create a venv in the ``./.venv`` path and install the development dependencies on it:
    ```console
    make devEnv
    ```

4. Use your favorite IDE to make your awesome changes and make sure the project is pointing to the venv generated.
To do that using Pycharm fo to the instructions [here](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html).

5. Then update and run the unit tests:

    ```console
    make test
    ```

6. Run pylint to be sure that we are using the best practices:

    ```console
    make lint
    ```

7. And finally commit and push your changes to your fork and the make a pull request to the original repo when you are ready to go.
Another member of the team will review your changes and after having two +1 you will be ready to merge them to the base repo.

    In order to sync your forked local version with the base repo you need to add an _upstream_ remote:

    ```console
    git remote add upstream https://github.com/mpi2/impc-etl.git
    ```
    
    Please procure to have your version in sync with the base repo to avoid merging hell.

    ```console
    git fetch upstream
    git checkout master
    git merge upstream/master
    git push origin master
    ```

## Re-generate the documentation


```
pdoc --html --force --template-dir docs/templates -o docs impc_etl
```


