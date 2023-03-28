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
        print('Loading media file: {}'.format(inFile))
        with open(inFile, 'r') as fh:
            self.mediaData = json.load(fh)

    def loadOmeroData(self, existingOmeroDataFolder):
        print('Loading existing omero data from: {}'.format(existingOmeroDataFolder))
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
                    print(key.lower())
                    self.existingOmeroData[key.lower()] = newEl

    def downloadImages(self, outFolder, outLog):
        missingImageElements = []
        for el in self.mediaData:
            site = el['centre'].lower()
            key = site + '/' + el['pipeline'] + '/' + el['procedure'] + '/' + el['parameter']
            fileKey = key + '/' + el['fileName']
            if fileKey.lower() in self.existingOmeroData:
                continue
            missingImageElements.append(el)

        print('Found {} images to download.'.format(len(missingImageElements)))
        with open(outLog, 'w') as logFh:
            for el in missingImageElements:
                site = el['centre'].lower()
                key = site + '/' + el['pipeline'] + '/' + el['procedure'] + '/' + el['parameter']

                outFile = join(outFolder + key, el['fileName'])
                if os.path.isfile(outFile):
                    continue

                response = requests.get(el['dccUrl'])
                if response.status_code == 200:
                    with open(outFile, 'wb') as outFileFh:
                        outFileFh.write(response.content)
                    logFh.write(el['centre'].lower() + ' :: ' + el['checksum'] + ' :: Y\n')
                    logFh.flush()
                else:
                    logFh.write(el['centre'].lower() + ' :: ' + el['checksum'] + ' :: N\n')
                    logFh.flush()

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
