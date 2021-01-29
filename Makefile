all: default

default: clean devDeps build

# submit-h: clean devDeps build
submit-h: clean build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-prod.cfg'  PYTHONPATH='.' PYSPARK_PYTHON=python36 YARN_CONF_DIR=/homes/federico/impc-etl/spark-2.4.5-bin-hadoop2.7/yarn-conf-hh/ luigi --module impc_etl.workflow.main $(task) --workers 3

submit-lsf: clean devDeps build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-lsf.cfg'  PYTHONPATH='.' luigi --module impc_etl.workflow.main $(task) --workers 3

submit-dev:
	LUIGI_CONFIG_PATH='luigi-dev.cfg'  PYTHONPATH='.' YARN_CONF_DIR='' luigi --module impc_etl.workflow.main $(task) --workers 2


.venv:          ##@environment Create a venv
	if [ ! -e "venv/bin/activate" ] ; then python -m venv --clear venv ; fi

lint:           ##@best_practices Run pylint against the main script and the shared, jobs and test folders
	source .venv/bin/activate && pylint -r n impc_etl/main.py impc_etl/shared/ impc_etl/jobs/ tests/

build: clean        ##@deploy Build to the dist package
	mkdir ./dist
#	cp ./impc_etl/main.py ./dist/
	zip -x main.py -r ./dist/impc_etl.zip impc_etl
	cd ./dist && mkdir libs
	source venv/bin/activate && pip install --upgrade pip
	source venv/bin/activate && pip install -U -r requirements/common.txt -t ./dist/libs
	source venv/bin/activate && pip install -U -r requirements/prod.txt -t ./dist/libs
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
	source venv/bin/activate && pip install -U -r requirements/common.txt
	source venv/bin/activate && pip install -U -r requirements/dev.txt

prodDeps: .venv      ##@deps Create a venv and install common and prod dependencies
	source venv/bin/activate && pip install -U -r requirements/common.txt
	source venv/bin/activate && pip install -U -r requirements/prod.txt


devEnv: .venv devDeps
##	source .venv/bin/activate && pip install -U pre-commit
##	source .venv/bin/activate && pre-commit install --install-hooks


data:            ##@data Download test data
	cd ${DATA_PATH} && mkdir imits mgi owl xml parquet solr misc
	cd ${DATA_PATH}/xml && mkdir impc 3i europhenome
	cp ${INPUT_DATA_PATH}/imits/imits-report.tsv ${DATA_PATH}/imits/
	cp ${INPUT_DATA_PATH}/imits/allele2Entries.tsv ${DATA_PATH}/imits/
	cp ${INPUT_DATA_PATH}/imits/productEntries.tsv ${DATA_PATH}/imits/
	cp -r ${INPUT_DATA_PATH}/3i/latest/*.xml ${DATA_PATH}/xml/3i/
	cd ${DATA_PATH}/xml/3i/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<ns2:/</g' {} \;
	cd ${DATA_PATH}/xml/3i/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<\/ns2:/<\//g' {} \;
	cp -r ${INPUT_DATA_PATH}/europhenome/2013-10-31/*.xml ${DATA_PATH}/xml/europhenome/
	cp -r ${INPUT_DATA_PATH}/europhenome/2013-05-20/*.xml ${DATA_PATH}/xml/europhenome/
	cd ${DATA_PATH}/xml/europhenome/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<ns2:/</g' {} \;
	cd ${DATA_PATH}/xml/europhenome/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<\/ns2:/<\//g' {} \;
	cp -r ${INPUT_DATA_PATH}/impc/latest/*/* ${DATA_PATH}/xml/impc/
	cd ${DATA_PATH}/xml/impc/ && find "$PWD" -type f -name "*.xml" -exec bash -c ' DIR=$( dirname "{}"  ); mv "{}" "$DIR"_$(basename "{}")  ' \;
	cd ${DATA_PATH}/xml/impc/ && rm -R -- */
	curl http://www.informatics.jax.org/downloads/reports/MGI_Strain.rpt --output ${DATA_PATH}/mgi/MGI_Strain.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_PhenotypicAllele.rpt --output ${DATA_PATH}/mgi/MGI_PhenotypicAllele.rpt
	curl http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt --output ${DATA_PATH}/mgi/MRK_List1.rpt
	curl http://www.informatics.jax.org/downloads/reports/HGNC_homologene.rpt --output ${DATA_PATH}/mgi/HGNC_homologene.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_GenePheno.rpt --output ${DATA_PATH}/mgi/MGI_GenePheno.rpt
	curl https://www.mousephenotype.org/embryoviewer/rest/ready --output ${DATA_PATH}/misc/embryo_data.json
#TODO Remove \n from embryo_data.json


test:       ##@best_practices Run pystest against the test folder
	source venv/bin/activate && pytest


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