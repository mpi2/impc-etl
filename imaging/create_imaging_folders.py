import json
import os
import sys
from os.path import join

def main(dataFile, outFolder):
    masterData = {}

    with open(dataFile, 'r') as fh:
        data = json.load(fh)
        for el in data:
            site = el['centre'].lower()
            siteData = {}
            if site in masterData:
                siteData = masterData[site]

            pipelineData = {}
            if el['pipelineKey'] in siteData:
                pipelineData = siteData[el['pipelineKey']]

            procedureData = []
            if el['procedureKey'] in pipelineData:
                procedureData = pipelineData[el['procedureKey']]

            if not el['parameterKey'] in procedureData:
                procedureData.append(el['parameterKey'])

            pipelineData[el['procedureKey']] = procedureData
            siteData[el['pipelineKey']] = pipelineData
            masterData[site] = siteData

    mode = 0o766
    for site in masterData:
        siteFolder = join(outFolder, site)
        siteData = masterData[site]

        if not os.path.isdir(siteFolder):
            os.mkdir(siteFolder, mode=mode)

        for pipelineKey in siteData:
            pipelineFolder = join(siteFolder, pipelineKey)
            pipelineData = siteData[pipelineKey]
            if not os.path.isdir(pipelineFolder):
                os.mkdir(pipelineFolder, mode=mode)

            for procedureKey in pipelineData:
                procedureFolder = join(pipelineFolder, procedureKey)
                procedureData = pipelineData[procedureKey]
                if not os.path.isdir(procedureFolder):
                    os.mkdir(procedureFolder, mode=mode)

                for paramKey in procedureData:
                    paramFolder = join(procedureFolder, paramKey)
                    if not os.path.isdir(paramFolder):
                        os.mkdir(paramFolder, mode=mode)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
