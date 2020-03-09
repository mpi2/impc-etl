all: default

default: clean devDeps build

submit: clean devDeps build
	LUIGI_CONFIG_PATH='luigi-prod.cfg'  PYTHONPATH='.' YARN_CONF_DIR=/Applications/spark-2.4.4-bin-hadoop2.7/yarn-conf/ luigi --module impc_etl.workflow.main ImpcEtl --workers 3

submit-dev:
	LUIGI_CONFIG_PATH='luigi-dev.cfg'  PYTHONPATH='.' YARN_CONF_DIR='' luigi --module impc_etl.workflow.main ImpcEtl --workers 2

.venv:          ##@environment Create a venv
	if [ ! -e ".venv/bin/activate" ] ; then python -m venv --clear .venv ; fi

lint:           ##@best_practices Run pylint against the main script and the shared, jobs and test folders
	source .venv/bin/activate && pylint -r n impc_etl/main.py impc_etl/shared/ impc_etl/jobs/ tests/

build: clean        ##@deploy Build to the dist package
	mkdir ./dist
#	cp ./impc_etl/main.py ./dist/
	zip -x main.py -r ./dist/impc_etl.zip impc_etl
	cd ./dist && mkdir libs
	source .venv/bin/activate && pip install --upgrade pip
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
	scp ${TEST_DATA_HOST}:${TEST_DATA_PATH}/imits/imits-report.tsv ./data/imits/
#	scp ${TEST_DATA_HOST}:${TEST_DATA_PATH}/imits/allele2Entries.tsv ./data/imits/
	scp -r ${TEST_DATA_HOST}:${TEST_DATA_PATH}/3i/latest/*.xml ./data/xml/3i/
	scp -r ${TEST_DATA_HOST}:${TEST_DATA_PATH}/europhenome/2013-10-31/*.xml ./data/xml/europhenome/
	scp  ${TEST_DATA_HOST}:${TEST_DATA_PATH}/impc/dr10.0/*/*.xml ./data/xml/impc/
	curl http://www.informatics.jax.org/downloads/reports/MGI_Strain.rpt --output ./data/mgi/MGI_Strain.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_PhenotypicAllele.rpt --output ./data/mgi/MGI_PhenotypicAllele.rpt
	curl http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt --output ./data/mgi/MRK_List1.rpt


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