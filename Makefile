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


data:            ##@data Download and structure input data for the ETL. Parameters: staging-path (e.g. /nobackup/staging), dr-tag (e.g. dr15.0), input-data-path (e.g. /impc/), etl-host (e.g. hadoop-host), etl-dir (e.g. /user/impc/)
	cd $(staging-path) && mkdir $(dr-tag)
	cd $(staging-path)/$(dr-tag) && mkdir tracking mgi ontologies xml parquet solr misc
	cd $(staging-path)/$(dr-tag)/xml && mkdir impc 3i europhenome
	cp $(input-data-path)/phenotype_data/imits/imits-phenotyping-colonies-report.tsv $(staging-path)/$(dr-tag)/tracking/
	cp $(input-data-path)/datafiles/daily/latest/gentar-phenotyping-colonies-report.tsv $(staging-path)/$(dr-tag)/tracking/
	cp $(input-data-path)/datafiles/daily/latest/allele2Entries.tsv $(staging-path)/$(dr-tag)/tracking/
	cp $(input-data-path)/datafiles/daily/latest/productEntries.tsv $(staging-path)/$(dr-tag)/tracking/
	cp $(input-data-path)/datafiles/daily/latest/gene_interest.tsv $(staging-path)/$(dr-tag)/tracking/
	cp $(input-data-path)/datafiles/daily/latest/mp2_load_gene_interest_report_es_cell.tsv $(staging-path)/$(dr-tag)/tracking/
	cp -r $(input-data-path)/impc_ontologies/*  $(staging-path)/$(dr-tag)/ontologies/
	cp -r $(input-data-path)/phenotype_data/3i/latest/*.xml $(staging-path)/$(dr-tag)/xml/3i/
	cd $(staging-path)/$(dr-tag)/xml/3i/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<ns2:/</g' {} \;
	cd $(staging-path)/$(dr-tag)/xml/3i/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<\/ns2:/<\//g' {} \;
	cp $(input-data-path)/datafiles/flow_results_EBIexport_180119.csv $(staging-path)/$(dr-tag)/misc/
	cp -r $(input-data-path)/phenotype_data/europhenome/2013-10-31/*.xml $(staging-path)/$(dr-tag)/xml/europhenome/
	cp -r $(input-data-path)/phenotype_data/europhenome/2013-05-20/*.xml $(staging-path)/$(dr-tag)/xml/europhenome/
	cd $(staging-path)/$(dr-tag)/xml/europhenome/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<ns2:/</g' {} \;
	cd $(staging-path)/$(dr-tag)/xml/europhenome/ && find ./*specimen*.xml -type f -exec sed -i -e 's/<\/ns2:/<\//g' {} \;
	cp -r $(input-data-path)/phenotype_data/impc/latest/* $(staging-path)/$(dr-tag)/xml/impc/
	cd $(staging-path)/$(dr-tag)/xml/impc/ && find "$$PWD" -type f -name "*.xml" -exec bash -c ' DIR=$$( dirname "{}"  ); mv "{}" "$$DIR"_$$(basename "{}")  ' \;
	cd $(staging-path)/$(dr-tag)/xml/impc/normal && find "$$PWD" -type d -empty -delete
	cd $(staging-path)/$(dr-tag)/xml/impc/finalising && find "$$PWD" -type d -empty -delete
	curl http://www.informatics.jax.org/downloads/reports/MGI_Strain.rpt --output $(staging-path)/$(dr-tag)/mgi/MGI_Strain.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_PhenotypicAllele.rpt --output $(staging-path)/$(dr-tag)/mgi/MGI_PhenotypicAllele.rpt
	curl http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt --output $(staging-path)/$(dr-tag)/mgi/MRK_List1.rpt
	curl http://www.informatics.jax.org/downloads/reports/HGNC_homologene.rpt --output $(staging-path)/$(dr-tag)/mgi/HGNC_homologene.rpt
	curl http://www.informatics.jax.org/downloads/reports/MGI_GenePheno.rpt --output $(staging-path)/$(dr-tag)/mgi/MGI_GenePheno.rpt
	curl https://www.mousephenotype.org/embryoviewer/rest/ready --output $(staging-path)/$(dr-tag)/misc/embryo_data_og.json
	cd $(staging-path)/$(dr-tag)/misc/ && tr -d '\n' < embryo_data_og.json > embryo_data.json
	cd $(staging-path)/$(dr-tag)/misc/ && rm embryo_data_og.json
	scp -r $(staging-path)/$(dr-tag) $(etl-host):$(etl-dir)/



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