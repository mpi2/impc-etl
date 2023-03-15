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

    def __init__(self, drTag, artefactsFolder, imagesFolder, logsFolder):
        self.drTag = drTag
        self.artefactsFolder = artefactsFolder
        self.imagesFolder = imagesFolder
        self.logsFolder = logsFolder
        imageDataFileSet = self.checkDataAvailable()
        if not self.valid:
            print(' - Unable to find data required to check upload status')

    def readImagesOnDisk(self):
        print(' -- Reading images available on disk ...')
        self.diskImageData = {}
        for site in os.listdir(self.imagesFolder):
            siteFolder = os.path.join(self.imagesFolder, site)
            for pipeline in os.listdir(siteFolder):
                pipelineFolder = os.path.join(siteFolder, pipeline)
                for procedure in os.listdir(pipelineFolder):
                    procedureFolder = os.path.join(pipelineFolder, procedure)
                    for parameter in os.listdir(procedureFolder):
                        parameterFolder = os.path.join(procedureFolder, parameter)
                        for imgFile in os.listdir(parameterFolder):
                            fileExtension = imgFile.rfind('.')
                            self.diskImageData[os.path.join(parameterFolder, imgFile)] = imgFile[fileExtension + 1:]
        print(' -- Found images: ' + str(len(self.diskImageData)))

    def checkDataAvailable(self):
        annotationsDataExists = False

        if os.path.exists(self.artefactsFolder + OmeroConstants.FILE_OMERO_ANNOTATIONS_DATA):
            annotationsDataExists = True

        imageDataFileSet = []
        if os.path.exists(self.artefactsFolder + self.drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX):
            for file in os.listdir(self.artefactsFolder + self.drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX):
                imageDataFileSet.append(
                    os.path.join(self.artefactsFolder + self.drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX,
                                 file))

        self.valid = annotationsDataExists and (len(imageDataFileSet) != 0)
        return imageDataFileSet


def main(drTag, artefactsFolder, imagesFolder, logsFolder):
    print(' -- [' + drTag + '] Using artefacts from: ' + artefactsFolder)
    print(' -- [' + drTag + '] Using images from: ' + imagesFolder)
    print(' -- [' + drTag + '] Using logs from: ' + logsFolder)
    checkOmeroUploadStatus = CheckOmeroUploadStatus(drTag, artefactsFolder, imagesFolder, logsFolder)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
