import os
import sys


class CheckUploadStatus:
    artefactsFolder = None
    imagesFolder = None
    logsFolder = None
    psPid = None
    imageData = {}

    def __init__(self, artefactsFolder, imagesFolder, logsFolder):
        self.artefactsFolder = artefactsFolder
        self.imagesFolder = imagesFolder
        self.logsFolder = logsFolder
        self.psPid = self.checkIfProcessIsStillRunning()
        if not self.psPid:
            print(' -- Upload process is no longer running!')
        else:
            print(' -- Upload process is still running. PID: ' + str(self.psPid))
            self.readImagesOnDisk()

    def checkIfProcessIsStillRunning(self):
        psOutput = os.popen('ps x | grep imaging').read()
        lines = psOutput.split('\n')

        psPid = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            segments = line.split(' ')
            actualSegments = []
            for seg in segments:
                seg = seg.strip()
                if seg:
                    actualSegments.append(seg)

            if not actualSegments:
                continue
            pid = actualSegments[0]
            for seg in actualSegments:
                if seg == 'imaging/upload_csv_to_omero.py':
                    psPid = pid
                    break
            if psPid:
                break

        return psPid

    def readImagesOnDisk(self):
        print(' -- Reading images available on disk ...')
        self.imageData = {}
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
                            self.imageData[os.path.join(parameterFolder, imgFile)] = imgFile[fileExtension + 1:]
        print(' -- Found images: ' + str(len(self.imageData)))


def main(artefactsFolder, imagesFolder, logsFolder):
    print(' -- Using artefacts from: ' + artefactsFolder)
    print(' -- Using images from: ' + imagesFolder)
    print(' -- Using logs from: ' + logsFolder)
    checkUploadStatus = CheckUploadStatus(artefactsFolder, imagesFolder, logsFolder)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
