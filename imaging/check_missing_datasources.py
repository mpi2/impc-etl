import json
import os
import sys
from os.path import join


def processPhenoCenter(inputFolder, site, dsData):
    entries = []
    siteFolder = join(inputFolder, site)

    for pipelineKey in os.listdir(siteFolder):
        pipelineFolder = join(siteFolder, pipelineKey)

        for procedureKey in os.listdir(pipelineFolder):
            procedureFolder = join(pipelineFolder, procedureKey)

            for parameterKey in os.listdir(procedureFolder):
                entries.append(site + '-' + pipelineKey + '-' + procedureKey + '-' + parameterKey)

    for entry in entries:
        if not entry in dsData:
            print('{}'.format(entry))


def main(inputFolder, jsonDatasourceFile):
    with open(jsonDatasourceFile, 'r') as fh:
        dsData = json.load(fh)

    for folder in os.listdir(inputFolder):
        processPhenoCenter(inputFolder, folder, dsData)


if __name__ == "__main__":
    main(sys.argv[1])
