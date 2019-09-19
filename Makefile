all: default

default: clean devDeps build

submit: clean devDeps build
	scp ./dist/impc_etl.zip ./dist/main.py ../../phenodcc-derived-parameters/target/phenodcc-derived-parameters-1.1.0.jar hadoop-login-01:~/
	ssh -t ${SPARK_HOST} spark2-submit --master yarn --conf spark.yarn.maxAppAttempts=1 --deploy-mode cluster --packages com.databricks:spark-xml_2.11:0.5.0,mysql:mysql-connector-java:5.1.38 --executor-memory 4G --jars phenodcc-derived-parameters-1.1.0.jar --driver-class-path phenodcc-derived-parameters-1.1.0.jar --py-files impc_etl.zip,libs.zip main.py  -d /user/federico/data/

.venv:          ##@environment Create a venv
	if [ ! -e ".venv/bin/activate" ] ; then python3 -m venv --clear .venv ; fi

lint:           ##@best_practices Run pylint against the main script and the shared, jobs and test folders
	source .venv/bin/activate && pylint -r n impc_etl/main.py impc_etl/shared/ impc_etl/jobs/ tests/

build: clean        ##@deploy Build to the dist package
	mkdir ./dist
#	cp ./impc_etl/main.py ./dist/
	zip -x main.py -r ./dist/impc_etl.zip impc_etl
	cd ./dist && mkdir libs
	source .venv/bin/activate && pip install -U -r requirements/common.txt -t ./dist/libs
	source .venv/bin/activate && pip install -U -r requirements/prod.txt -t ./dist/libs
	cd ./dist/libs && zip -r ../libs.zip .
	cd ./dist && rm -rf libs


clean: clean-build clean-pyc clean-test           ##@clean Clean all

clean-build:           ##@clean Clean the dist folder
	rm -fr dist/

clean-pyc:           ##@clean Clean all the python auto generated files
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:           ##@clean Clean the pytest cache
	rm -fr .pytest_cache

devDeps: .venv      ##@deps Create a venv and install common and dev dependencies
	source .venv/bin/activate && pip install -U -r requirements/common.txt
	source .venv/bin/activate && pip install -U -r requirements/dev.txt

prodDeps: .venv      ##@deps Create a venv and install common and prod dependencies
	source .venv/bin/activate && pip install -U -r requirements/common.txt
	source .venv/bin/activate && pip install -U -r requirements/prod.txt


devEnv: .venv devDeps
##	source .venv/bin/activate && pip install -U pre-commit
##	source .venv/bin/activate && pre-commit install --install-hooks


download:            ##@download Download test data
	scp ${TEST_DATA_HOST}:${TEST_DATA_PATH}/imits/imits-report.tsv ./tests/data/imits/
	scp ${TEST_DATA_HOST}:${TEST_DATA_PATH}/imits/allele2Entries.tsv ./tests/data/imits/
	scp -r ${TEST_DATA_HOST}:${TEST_DATA_PATH}/3i/latest/ ./tests/data/3i/
	scp -r ${TEST_DATA_HOST}:${TEST_DATA_PATH}/europhenome/2013-10-31/ ./tests/data/europhenome/
	scp  ${TEST_DATA_HOST}:${TEST_DATA_PATH}/impc/latest/*/*.xml ./tests/data/dcc/
	curl http://www.informatics.jax.org/downloads/reports/MGI_Strain.rpt --output ./tests/data/mgi/MGI_Strain.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_GenePheno.rpt --output ./tests/data/mgi/MGI_GenePheno.rpt


test:       ##@best_practices Run pystest against the test folder
	source .venv/bin/activate && pytest


 HELP_FUN = \
		 %help; \
		 while(<>) { push @{$$help{$$2 // 'options'}}, [$$1, $$3] if /^(\w+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/ }; \
		 print "usage: make [target]\n\n"; \
	 for (keys %help) { \
		 print "$$_:\n"; $$sep = " " x (20 - length $$_->[0]); \
		 print "  $$_->[0]$$sep$$_->[1]\n" for @{$$help{$$_}}; \
		 print "\n"; }


help:
	@echo "Run 'make' without a target to clean all, install dev dependencies, test, lint and build the package \n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)