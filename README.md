# IMPC ETL Process
IMPC Extraction, Transformation and Loading process to generate the data that supports [mousephenotype.org](http://mousephenotype.org) among with other internal processes.
## Requirements
- Python 3+
- Spark 2+
- make

## How to run it
Download the latest release package from the [releases page](https://github.com/mpi2/impc-etl/releases) and decompress it.
The submit your job to your Spark 2 cluster using:

```console
spark-submit --py-files jobs.zip,shared.zip,libs.zip impc_etl.py
```



## Development environment setup
Fork this repo and then clone your forked version:
```console
git clone https://github.com/USERNAME/impc-etl.git
cd impc-etl
```

Run _make_ to create a venv in the ``./.venv`` path and install the development dependencies on it:
```console
make devEnv
```

Use your favorite IDE to make your awesome changes and make sure the project is pointing to the venv generated.
To do that using Pycharm fo to the instructions [here](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html).

Then update and run the unit tests:

```console
make test
```

Run pylint to be sure that we are using the best practices:

```console
make lint
```

And finally commit and push your changes to your fork and the make a pull request to the original repo when you are ready to go.
Another member of the team will review your changes and after having two +1 you will be ready to merge them to the base repo.

In order to sync your forked local version with the base repo you need to add an _upstream_ remote:

```console
git remote add upstream https://github.com/mpi2/impc-etl.git
git fetch upstream
git checkout master
git merge upstream/master
```

Please procure to have your version in sync with the base repo to avoid merging hell.




