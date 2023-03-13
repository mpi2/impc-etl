echo `hostname`
echo "${WORKSPACE}"

# Check if DATA_RELEASE parameter supplied
if [ ${#DATA_RELEASE} -ne "6" ]; then
	echo "Did not receive expected value for DATA_RELEASE parameter. Expected something of the form dr_xx e.g. dr_08. Got: ${DATA_RELEASE}. Exiting"
    exit 1
fi

## Updating the ETL pipeline to the latest
COMMAND="cd ${ETL_PATH} && git pull origin master"
ssh mi_hadoop@codon-login ${COMMAND}

## Create the appropriate data folders related to the imaging pipeline on Codon
COMMAND="cd ${ETL_PATH} && make imaging-data-media dr-tag=${DATA_RELEASE} staging-path=${CODON_STAGING_AREA} input-data-path=${DATA_RELEASES_ARCHIVE_AREA} target-date=${TARGET_DATE}"
ssh mi_hadoop@codon-login ${COMMAND}

## Prepare the environment for the imaging pipeline on komp-jenkins
COMMAND="if [ ! -d \"${IMAGING_HOLDING_AREA}/${DATA_RELEASE}\" ]; then mkdir ${IMAGING_HOLDING_AREA}/${DATA_RELEASE}; fi"
ssh mi_adm@komp-jenkins ${COMMAND}

## Download the pipeline code
COMMAND="if [ -d \"${IMAGING_HOLDING_AREA}/${DATA_RELEASE}/impc-etl\" ]; then cd ${IMAGING_HOLDING_AREA}/${DATA_RELEASE}/impc-etl && git pull origin master; else cd ${IMAGING_HOLDING_AREA}/${DATA_RELEASE} && git clone https://github.com/mpi2/impc-etl.git; fi"
ssh mi_adm@komp-jenkins ${COMMAND}

## Run the imaging download pipeline
COMMAND="cd ${IMAGING_HOLDING_AREA}/${DATA_RELEASE}/impc-etl && make imaging-data-download codon-staging=${CODON_STAGING_AREA} staging-path=${IMAGING_HOLDING_AREA} dr-tag=${DATA_RELEASE}"
ssh mi_adm@komp-jenkins ${COMMAND} &
