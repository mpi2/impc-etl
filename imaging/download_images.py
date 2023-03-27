import json
import os
import sys
from os.path import join

import requests


class DownloadImages:
    mediaData = []
    existingOmeroData = {}

    def __init__(self, inFile, existingOmeroDataFolder):
        self.mediaData = {}
        self.existingOmeroData = {}
        self.loadMediaData(inFile)
        self.loadOmeroData(existingOmeroDataFolder)

    def loadMediaData(self, inFile):
        with open(inFile, 'r') as fh:
            self.mediaData = json.load(fh)

    def loadOmeroData(self, existingOmeroDataFolder):
        for file in os.listdir(existingOmeroDataFolder):
            with open(os.path.join(existingOmeroDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)
                for el in jsonData:
                    folderPath = el['path'].split('impc/')[-1]
                    idx = folderPath.rfind('/')
                    newEl = {
                        'fileName': el['name'],
                        'folderPath': folderPath[:idx]
                    }
                    key = newEl['folderPath'] + '/' + newEl['fileName']
                    self.existingOmeroData[key.lower()] = newEl

    def downloadImages(self, outFolder, outLog):
        with open(outLog, 'w') as logFh:
            count = 1
            for el in self.mediaData:
                site = el['centre'].lower()
                key = site + '/' + el['pipelineKey'] + '/' + el['procedureKey'] + '/' + el[
                    'parameterKey']
                imgFolder = outFolder + key
                toDownload = el['dccUrl']
                fileName = str(el['id']) + '.' + el['extension']
                fileKey = key + '/' + fileName
                if fileKey in self.existingOmeroData:
                    continue

                outFile = join(imgFolder, fileName)
                if os.path.isfile(outFile):
                    count += 1
                    continue

                response = requests.get(toDownload)
                if response.status_code == 200:
                    with open(outFile, 'wb') as outFileFh:
                        outFileFh.write(response.content)
                    logFh.write(el['centre'].lower() + ' :: ' + el['checksum'] + ' :: Y\n')
                    logFh.flush()
                else:
                    logFh.write(el['centre'].lower() + ' :: ' + el['checksum'] + ' :: N\n')
                    logFh.flush()

                count += 1

    def removeEmptyFolders(self, imagesFolder):
        print('Looking for empty folders to remove ...')
        for folder in os.listdir(imagesFolder):
            centerFolder = os.path.join(imagesFolder, folder)
            if len(os.listdir(centerFolder)) == 0:
                print('- Folder [' + centerFolder + '] is empty. Removing folder ...')
                os.rmdir(centerFolder)
            else:
                for pipeline in os.listdir(centerFolder):
                    pipelineFolder = os.path.join(centerFolder, pipeline)
                    if len(os.listdir(pipelineFolder)) == 0:
                        print('- Folder [' + pipelineFolder + '] is empty. Removing folder ...')
                        os.rmdir(pipelineFolder)
                    else:
                        for procedure in os.listdir(pipelineFolder):
                            procedureFolder = os.path.join(pipelineFolder, procedure)
                            if len(os.listdir(procedureFolder)) == 0:
                                print('- Folder [' + procedureFolder + '] is empty. Removing folder ...')
                                os.rmdir(procedureFolder)
                            else:
                                for parameter in os.listdir(procedureFolder):
                                    parameterFolder = os.path.join(procedureFolder, parameter)
                                    if len(os.listdir(parameterFolder)) == 0:
                                        print(
                                            '- Folder [' + parameterFolder + '] is empty. Removing folder ...')
                                        os.rmdir(parameterFolder)
                                if len(os.listdir(procedureFolder)) == 0:
                                    print('- Folder [' + procedureFolder + '] is empty. Removing folder ...')
                                    os.rmdir(procedureFolder)
                        if len(os.listdir(pipelineFolder)) == 0:
                            print('- Folder [' + pipelineFolder + '] is empty. Removing folder ...')
                            os.rmdir(pipelineFolder)
                if len(os.listdir(centerFolder)) == 0:
                    print('- Folder [' + centerFolder + '] is empty. Removing folder ...')
                    os.rmdir(centerFolder)


def main(inFile, existingOmeroDataFolder, outFolder, outLog):
    downloadImages = DownloadImages(inFile, existingOmeroDataFolder)
    downloadImages.downloadImages(outFolder, outLog)
    downloadImages.removeEmptyFolders(outFolder)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
