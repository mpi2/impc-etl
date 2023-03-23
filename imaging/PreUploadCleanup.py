import json
import logging
import os
import shutil

from imaging import OmeroConstants


class PreUploadCleanup:
    drTag = None
    artefactsFolder = None
    imagesFolder = None
    imagingCleanPath = None

    def __init__(self, drTag, artefactsFolder, imagesFolder, imagingCleanPath):
        self.logger = logging.getLogger(__name__)
        self.drTag = drTag
        self.artefactsFolder = artefactsFolder
        self.imagesFolder = imagesFolder
        self.imagingCleanPath = imagingCleanPath

    def createFoldersInClean(self):
        csvFile = self.artefactsFolder + self.drTag + '.csv'
        mode = 0o766

        self.logger.info('Checking if additional folders have to be created in clean ...')
        with open(csvFile, 'r') as fh:
            lines = fh.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith('observation_id'):
                continue

            segs = line.split(',')
            phenotyping_center = segs[3]
            pipeline_stable_id = segs[4]
            procedure_stable_id = segs[5]
            parameter_stable_id = segs[7]

            siteFolder = os.path.join(self.imagingCleanPath, phenotyping_center)
            if not os.path.isdir(siteFolder):
                self.logger.info('Creating folder: ' + siteFolder)
                os.mkdir(siteFolder, mode)
            pipelineFolder = os.path.join(siteFolder, pipeline_stable_id)
            if not os.path.isdir(pipelineFolder):
                self.logger.info('Creating folder: ' + pipelineFolder)
                os.mkdir(pipelineFolder, mode)
            procedureFolder = os.path.join(pipelineFolder, procedure_stable_id)
            if not os.path.isdir(procedureFolder):
                self.logger.info('Creating folder: ' + procedureFolder)
                os.mkdir(procedureFolder, mode)
            parameterFolder = os.path.join(procedureFolder, parameter_stable_id)
            if not os.path.isdir(parameterFolder):
                self.logger.info('Creating folder: ' + parameterFolder)
                os.mkdir(parameterFolder, mode)

    # Reading CSV file and looking for files that won't be uploaded anyway and move them to clean:
    # ['.mov', '.bin', '.fcs', '.nrrd', '.bz2', '.arf', '.qt']
    def cleanUpCSV(self):
        undesired_extensions = ['mov', 'bin', 'fcs', 'nrrd', 'bz2', 'arf', 'qt']
        csvFile = self.artefactsFolder + self.drTag + '.csv'

        self.logger.info('Removing files that won\'t be uploaded from the CSV file ...')
        toKeep = []
        entriesRemoved = False
        with open(csvFile, 'r') as fh:
            lines = fh.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith('observation_id'):
                toKeep.append(line)
                continue

            segs = line.split(',')
            downloadUrl = segs[2]
            phenotyping_center = segs[3]
            pipeline_stable_id = segs[4]
            procedure_stable_id = segs[5]
            parameter_stable_id = segs[7]
            fileName = os.path.split(downloadUrl)[-1]
            extension = fileName.split('.')[-1]
            key = os.path.join(phenotyping_center, pipeline_stable_id, procedure_stable_id, parameter_stable_id,
                               fileName)
            cleanFolderPath = os.path.join(self.imagingCleanPath, phenotyping_center, pipeline_stable_id,
                                           procedure_stable_id, parameter_stable_id)

            if extension in undesired_extensions:
                fullFilePath = os.path.join(self.imagesFolder, key)
                entriesRemoved = True
                if os.path.exists(fullFilePath):
                    self.logger.info('Moving: ' + fullFilePath)
                    if not os.path.isdir(cleanFolderPath):
                        self.logger.warning(' -- Cannot move file. Folder [' + cleanFolderPath + '] does not exist!')
                    else:
                        finalFilePath = os.path.join(cleanFolderPath, fileName)
                        shutil.move(fullFilePath, finalFilePath)
                else:
                    self.logger.warning('File [' + fullFilePath + '] does not exist!')
            else:
                toKeep.append(line)

        if entriesRemoved:
            os.remove(csvFile)
            with open(csvFile, 'w') as fh:
                fh.write('\n'.join(toKeep))

    def moveDataAlreadyUploaded(self):
        self.logger.info('Checking ' + self.drTag + ' files for entries already uploaded ...')
        baseImagesFolder = os.path.join(self.artefactsFolder, OmeroConstants.FOLDER_OMERO_IMAGES_DATA)
        drDataFile = os.path.join(baseImagesFolder, self.drTag + OmeroConstants.FILE_OMERO_IMAGES_DATA)

        toBeRemovedFromCSV = []
        if os.path.exists(drDataFile):
            with open(drDataFile, 'r') as fh:
                jsonData = json.load(fh)

                for el in jsonData:
                    localPath = os.path.join(self.imagesFolder, el['path'].split('impc/')[-1])
                    cleanPath = '/' + el['path']
                    if os.path.exists(localPath):
                        self.logger.info('Moving: ' + localPath + ' to: ' + cleanPath)
                        cleanFolder = cleanPath[:cleanPath.rfind('/')]
                        if not os.path.isdir(cleanFolder):
                            self.logger.warning(
                                'Unable to move [' + localPath + ']. Folder [' + cleanFolder + '] does not exist!')
                        else:
                            shutil.move(localPath, cleanPath)
                            entry = el['path'].split('impc/')[-1]
                            toBeRemovedFromCSV.append(entry.lower())

        self.logger.info('Found ' + str(len(toBeRemovedFromCSV)) + ' to be removed from the CSV.')
        self.removeEmptyFolders()

        if len(toBeRemovedFromCSV) > 0:
            csvFile = self.artefactsFolder + self.drTag + '.csv'
            toKeep = []
            with open(csvFile, 'r') as fh:
                lines = fh.readlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('observation_id'):
                    toKeep.append(line)
                    continue

                segs = line.split(',')
                downloadUrl = segs[2]
                phenotyping_center = segs[3]
                pipeline_stable_id = segs[4]
                procedure_stable_id = segs[5]
                parameter_stable_id = segs[7]
                fileName = os.path.split(downloadUrl)[-1]
                key = os.path.join(phenotyping_center, pipeline_stable_id, procedure_stable_id, parameter_stable_id,
                                   fileName)
                if not key.lower() in toBeRemovedFromCSV:
                    toKeep.append(line)
            os.remove(csvFile)
            with open(csvFile, 'w') as fh:
                fh.write('\n'.join(toKeep))

    def removeEmptyFolders(self):
        self.logger.info('Looking for empty folders to remove ...')
        for folder in os.listdir(self.imagesFolder):
            centerFolder = os.path.join(self.imagesFolder, folder)
            if len(os.listdir(centerFolder)) == 0:
                self.logger.info('- Folder [' + centerFolder + '] is empty. Removing folder ...')
                os.rmdir(centerFolder)
            else:
                for pipeline in os.listdir(centerFolder):
                    pipelineFolder = os.path.join(centerFolder, pipeline)
                    if len(os.listdir(pipelineFolder)) == 0:
                        self.logger.info('- Folder [' + pipelineFolder + '] is empty. Removing folder ...')
                        os.rmdir(pipelineFolder)
                    else:
                        for procedure in os.listdir(pipelineFolder):
                            procedureFolder = os.path.join(pipelineFolder, procedure)
                            if len(os.listdir(procedureFolder)) == 0:
                                self.logger.info('- Folder [' + procedureFolder + '] is empty. Removing folder ...')
                                os.rmdir(procedureFolder)
                            else:
                                for parameter in os.listdir(procedureFolder):
                                    parameterFolder = os.path.join(procedureFolder, parameter)
                                    if len(os.listdir(parameterFolder)) == 0:
                                        self.logger.info(
                                            '- Folder [' + parameterFolder + '] is empty. Removing folder ...')
                                        os.rmdir(parameterFolder)
