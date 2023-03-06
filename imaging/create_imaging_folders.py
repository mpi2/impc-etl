import json
import os
import sys
from os.path import join

SITES = [
    ('bcm', 'BCM',),
    ('gmc', 'HMGU',),
    ('h', 'MRC Harwell'),
    ('ics', 'ICS',),
    ('j', 'JAX',),
    ('tcp', 'TCP'),
    ('ning', 'NING',),
    ('rbrc', 'RBRC',),
    ('ucd', 'UC Davis',),
    ('wtsi', 'WTSI',),
    ('kmpc', 'KMPC',),
    ('ccpcz', 'CCP-IMG',),
]

def main(dataFile, outFolder):
    masterData = {}

    with open(dataFile, 'r') as fh:
        data = json.load(fh)
        for el in data:
            site = SITES[el['centre'].lower()]
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

    mode = 0o666
    for site in masterData:
        siteFolder = join(outFolder, site)
        siteData = masterData[site]
        os.mkdir(siteFolder, mode=mode)

        for pipelineKey in siteData:
            pipelineFolder = join(siteFolder, pipelineKey)
            pipelineData = siteData[pipelineKey]
            os.mkdir(pipelineFolder, mode=mode)

            for procedureKey in pipelineData:
                procedureFolder = join(pipelineFolder, procedureKey)
                procedureData = pipelineData[procedureKey]
                os.mkdir(procedureFolder, mode=mode)

                for paramKey in procedureData:
                    paramFolder = join(procedureFolder, paramKey)
                    os.mkdir(paramFolder, mode=mode)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
