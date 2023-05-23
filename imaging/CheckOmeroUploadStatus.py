import json
import os
import sys

from imaging import OmeroConstants


class CheckOmeroUploadStatus:
    drTag = None
    artefactsFolder = None
    imagesFolder = None
    logsFolder = None
    valid = False
    diskImageData = {}

    def __init__(self, drTag, artefactsFolder):
        self.drTag = drTag
        self.artefactsFolder = artefactsFolder
        self.checkExistingData()

    def checkExistingData(self):
        csvFile = self.artefactsFolder + self.drTag + '.csv'
        csvExists = False
        if os.path.exists(csvFile):
            csvExists = True
        else:
            print('ERROR: Cannot find CSV file: ' + csvFile)

        drDataFileExists = False
        baseImagesFolder = os.path.join(self.artefactsFolder, OmeroConstants.FOLDER_OMERO_IMAGES_DATA)
        drDataFile = os.path.join(baseImagesFolder, self.drTag + OmeroConstants.FILE_OMERO_IMAGES_DATA)
        if os.path.exists(drDataFile):
            drDataFileExists = True
        else:
            print('ERROR: Cannot find DR file: ' + drDataFile)
        self.valid = csvExists and drDataFileExists

    def isValid(self):
        return self.valid

    def checkProgress(self):
        csvFile = self.artefactsFolder + self.drTag + '.csv'
        with open(csvFile, 'r') as fh:
            lines = fh.readlines()
        csvEntries = len(lines) - 1

        baseImagesFolder = os.path.join(self.artefactsFolder, OmeroConstants.FOLDER_OMERO_IMAGES_DATA)
        drDataFile = os.path.join(baseImagesFolder, self.drTag + OmeroConstants.FILE_OMERO_IMAGES_DATA)
        with open(drDataFile, 'r') as fh:
            jsonData = json.load(fh)
        jsonEntries = len(jsonData)

        print('- Current status: ' + str(jsonEntries) + ' of ' + str(csvEntries))


def main(drTag, artefactsFolder):
    print(' -- [' + drTag + '] Using artefacts from: ' + artefactsFolder)
    checkOmeroUploadStatus = CheckOmeroUploadStatus(drTag, artefactsFolder)
    if not checkOmeroUploadStatus.isValid():
        print('ERROR: Unable to find relevant data!')
    else:
        checkOmeroUploadStatus.checkProgress()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
