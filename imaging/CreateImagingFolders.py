import json
import os
import sys
from os.path import join


class CreateImagingFolders:
    masterData = {}

    def __init__(self, dataFile):
        self.masterData = {}
        self.prepareData(dataFile)

    def prepareData(self, dataFile):
        with open(dataFile, 'r') as fh:
            data = json.load(fh)
            for el in data:
                site = el['centre']
                siteData = {}
                if site in self.masterData:
                    siteData = self.masterData[site]

                pipelineData = {}
                if el['pipeline'] in siteData:
                    pipelineData = siteData[el['pipeline']]

                procedureData = []
                if el['procedure'] in pipelineData:
                    procedureData = pipelineData[el['procedure']]

                if not el['parameter'] in procedureData:
                    procedureData.append(el['parameter'])

                pipelineData[el['procedure']] = procedureData
                siteData[el['pipeline']] = pipelineData
                self.masterData[site] = siteData

    def createFolders(self, outFolder):
        mode = 0o766
        print('Creating folders ...')
        count = 0
        for site in self.masterData:
            siteFolder = join(outFolder, site)
            siteData = self.masterData[site]

            if not os.path.isdir(siteFolder):
                os.mkdir(siteFolder, mode=mode)
                count += 1

            for pipelineKey in siteData:
                pipelineFolder = join(siteFolder, pipelineKey)
                pipelineData = siteData[pipelineKey]
                if not os.path.isdir(pipelineFolder):
                    os.mkdir(pipelineFolder, mode=mode)
                    count += 1

                for procedureKey in pipelineData:
                    procedureFolder = join(pipelineFolder, procedureKey)
                    procedureData = pipelineData[procedureKey]
                    if not os.path.isdir(procedureFolder):
                        os.mkdir(procedureFolder, mode=mode)
                        count += 1

                    for paramKey in procedureData:
                        paramFolder = join(procedureFolder, paramKey)
                        if not os.path.isdir(paramFolder):
                            os.mkdir(paramFolder, mode=mode)
                            count += 1
        print('Created {} folders ...'.format(count))


def main(dataFile, outFolder):
    createImagingFolders = CreateImagingFolders(dataFile)
    createImagingFolders.createFolders(outFolder)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
