import csv
import datetime
import glob
import json
import logging
import os
import os.path
import sys
import time

from imaging import OmeroConstants
from imaging.OmeroFileService import OmeroFileService
from imaging.OmeroProperties import OmeroProperties
from imaging.OmeroService import OmeroService
from imaging.PreUploadCleanup import PreUploadCleanup

LOAD_WHOLE_DIR_THRESHOLD = 300


class UploadCSVToOmero:
    omeroFileService = None
    omeroService = None
    files_to_upload_available = []
    omero_dir_list = []
    dict_nfs_filenames = {}

    def __init__(self, artefactsFolder, omeroDevPropetiesFile):
        self.logger = logging.getLogger(__name__)
        if not self.checkMissingDSFile(artefactsFolder + OmeroConstants.FILE_MISSING_DATASOURCES):
            sys.exit(-1)

        omeroProperties = OmeroProperties(omeroDevPropetiesFile)
        self.omeroFileService = OmeroFileService(omeroProperties.getProperties())
        self.omeroService = OmeroService(omeroProperties.getProperties())

    def checkMissingDSFile(self, missingDSFile):
        if os.path.isfile(missingDSFile):
            self.logger.error(
                'ERROR: Unable to start the upload process as there still are missing datasources: ' + missingDSFile)
            return False
        return True

    def prepareData(self, drTag, artefactsFolder, imagesFolder):
        csv_directory_to_filenames_map = self.loadCSVFile(artefactsFolder + drTag + '.csv')
        omero_file_list = self.buildOmeroBaseFileList(artefactsFolder)
        newer_data_file_list = self.loadNewDataReleases(artefactsFolder)
        omero_file_list.extend(newer_data_file_list)
        self.omero_dir_list = list(set([os.path.split(f)[0] for f in omero_file_list]))
        self.prepareDataToBeUploaded(imagesFolder, csv_directory_to_filenames_map, omero_file_list)

    def loadCSVFile(self, csvFile):
        self.logger.info('Using CSV file for Omero upload: ' + csvFile)
        csv_directory_to_filenames_map = {}
        n_from_csv_file = 0
        with open(csvFile, 'r') as fid:
            csv_reader = csv.reader(fid)
            header_row = csv_reader.next()
            try:
                download_file_path_idx = header_row.index("download_file_path")
                phenotyping_center_idx = header_row.index("phenotyping_center")
                pipeline_stable_idx = header_row.index("pipeline_stable_id")
                procedure_stable_idx = header_row.index("procedure_stable_id")
                parameter_stable_idx = header_row.index("parameter_stable_id")
            except ValueError as e:
                self.logger.error('ERROR reading CSV file: ' + str(e))
                sys.exit(-1)

            for row in csv_reader:
                download_file_path = row[download_file_path_idx].lower()
                if download_file_path.find('mousephenotype.org') < 0 or \
                        download_file_path.endswith('.mov') or \
                        download_file_path.endswith('.bin') or \
                        download_file_path.endswith('.fcs') or \
                        download_file_path.endswith('.nrrd') or \
                        download_file_path.endswith('.bz2') or \
                        download_file_path.endswith('.arf'):
                    continue

                phenotyping_center = row[phenotyping_center_idx]
                pipeline_stable_id = row[pipeline_stable_idx]
                procedure_stable_id = row[procedure_stable_idx]
                parameter_stable_id = row[parameter_stable_idx]
                if len(phenotyping_center) == 0 or \
                        len(pipeline_stable_id) == 0 or \
                        len(procedure_stable_id) == 0 or \
                        len(parameter_stable_id) == 0:
                    self.logger.warning("Did not receive a required field - " + \
                                        "phenotyping_center='" + phenotyping_center + \
                                        "', pipeline_stable_id='" + pipeline_stable_id + \
                                        "', procedure_stable_id='" + procedure_stable_id + \
                                        "', parameter_stable_id='" + parameter_stable_id + \
                                        "' - not uploading: " + download_file_path)
                    continue
                fname = os.path.split(download_file_path)[-1]
                key = os.path.join(phenotyping_center, pipeline_stable_id, procedure_stable_id, parameter_stable_id,
                                   fname)
                csv_directory_to_filenames_map[key] = download_file_path
                n_from_csv_file += 1
        self.logger.info('Found ' + str(n_from_csv_file) + ' records to be uploaded to Omero.')
        return csv_directory_to_filenames_map

    def buildOmeroBaseFileList(self, artefactsFolder):
        imageDataExists = self.omeroFileService.checkImageDataOnDisk(
            artefactsFolder + OmeroConstants.FOLDER_OMERO_IMAGES_DATA,
            artefactsFolder + OmeroConstants.FILE_OMERO_IMAGE_DATA_LIST)
        self.logger.info('Base Omero image data on disk: ' + str(imageDataExists))
        if not imageDataExists:
            self.logger.info('ERROR: Bse Omero image data missing !!!')
            sys.exit(-1)

        self.logger.info('Loading base Omero image data from disk ...')
        omero_file_list = self.omeroFileService.loadImageDataFromFile(
            artefactsFolder + OmeroConstants.FILE_OMERO_IMAGE_DATA_LIST)
        self.logger.info('Found ' + str(len(omero_file_list)) + ' base images in Omero.')
        return omero_file_list

    def loadNewDataReleases(self, artefactsFolder):
        self.logger.info('Looking for new data releases ...')
        newDRDataFiles = []
        imagesDir = os.path.join(artefactsFolder, OmeroConstants.FOLDER_OMERO_IMAGES_DATA)
        for file in os.listdir(imagesDir):
            if file.startswith('dr'):
                newDRDataFiles.append(os.path.join(imagesDir, file))

        self.logger.info('Found ' + str(len(newDRDataFiles)) + ' new data releases ...')
        fileList = []
        for newDRFile in newDRDataFiles:
            with open(newDRFile, 'r') as fh:
                jsonData = json.load(fh)

            for el in jsonData:
                fileList.append(el['path'].split('impc/')[-1])
        self.logger.info(
            'Found ' + str(len(fileList)) + ' images in ' + str(len(newDRDataFiles)) + ' new data releases ...')
        return fileList

    def prepareDataToBeUploaded(self, imagesFolder, csv_directory_to_filenames_map, omero_file_list):
        # Get the files in NFS
        list_nfs_filenames = []
        self.logger.info('Retrieving list of files on disk ...')
        os.path.walk(imagesFolder, self.add_to_list, list_nfs_filenames)
        list_nfs_filenames = [f.split(imagesFolder)[-1] for f in list_nfs_filenames]
        self.logger.info('Found ' + str(len(list_nfs_filenames)) + ' files on disk.')

        # Modified to carry out case-insensitive comparisons.
        set_csv_filenames = set([k.lower() for k in csv_directory_to_filenames_map.keys()])
        set_omero_filenames = set([f.lower() for f in omero_file_list])
        self.dict_nfs_filenames = {}
        for f in list_nfs_filenames:
            # Note that if more than one file maps to the same case insensitive value
            # only the last one encountered will be used
            self.dict_nfs_filenames[f.lower()] = f
        set_nfs_filenames = set(self.dict_nfs_filenames.keys())

        files_to_upload = set_csv_filenames - set_omero_filenames
        self.files_to_upload_available = files_to_upload.intersection(set_nfs_filenames)
        files_to_upload_unavailable = files_to_upload - self.files_to_upload_available

        self.logger.info('Number of files to upload: ' + str(len(files_to_upload)))
        self.logger.info('Number of files to available: ' + str(len(self.files_to_upload_available)))
        self.logger.warning('Number of files unavailable for upload: ' + str(len(files_to_upload_unavailable)))

    def add_to_list(self, L, dirname, names):
        for n in names:
            fullname = os.path.join(dirname, n)
            if os.path.isfile(fullname):
                L.append(fullname)

    def doUpload(self, drTag, artefactsFolder, imagesFolder):

        # Create a dictionary for the files to upload with the directory as the
        # key and the original nfs filenames as the values, so each dir can be passed to
        # omero with associated files
        dict_files_to_upload = {}
        for f in self.files_to_upload_available:
            dirname, filename = os.path.split(self.dict_nfs_filenames[f])
            if dict_files_to_upload.has_key(dirname):
                dict_files_to_upload[dirname].append(filename)
            else:
                dict_files_to_upload[dirname] = [filename]

        # Upload files
        n_dirs_to_upload = len(dict_files_to_upload)
        for index, directory in zip(range(n_dirs_to_upload), dict_files_to_upload.keys()):
            filenames = dict_files_to_upload[directory]
            n_files_to_upload = len(filenames)
            self.logger.info('Uploading: [' + str(index + 1) + ' of ' + str(n_dirs_to_upload) + ']: ' + directory)
            dir_structure = directory.split('/')
            project = dir_structure[0]
            # Below we assume dir_structure is list with elements:
            # [project, pipeline, procedure, parameter]
            dataset = "-".join(dir_structure)
            fullpath = os.path.join(imagesFolder, directory)

            # if dir contains pdf file we cannot load whole directory
            if len(glob.glob(os.path.join(fullpath, '*.pdf'))) > 0:
                self.logger.info(' -- Uploading PDFs ...')
                self.omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=filenames)
            else:
                # Check if the dir is in omero.
                # If not we can import the whole dir irrespective of number of files
                dir_not_in_omero = True
                try:
                    if self.omero_dir_list.index(directory) >= 0:
                        dir_not_in_omero = False
                except ValueError:
                    pass
                self.logger.info(' -- Uploading images ...')
                if dir_not_in_omero or n_files_to_upload > LOAD_WHOLE_DIR_THRESHOLD:
                    self.omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=None)
                else:
                    self.omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=filenames)

            newImageData = self.omeroFileService.retrieveImagesFromOmero(drTag)
            newAnnotationData = self.omeroFileService.retrieveAnnotationsFromOmero(drTag)
            newData = []
            for el in newImageData:
                el['path'] = el['path'].replace('holding_area/impc/' + drTag + '/images', 'clean/impc')
                newData.append(el)
            for el in newAnnotationData:
                el['path'] = el['path'].replace('holding_area/impc/' + drTag + '/images', 'clean/impc')
                newData.append(el)
            self.logger.info('Added ' + str(len(newData)) + ' image entries.')
            if len(newData) > 0:
                self.omeroFileService.runUpdate(drTag)
                baseImagesFolder = os.path.join(artefactsFolder, OmeroConstants.FOLDER_OMERO_IMAGES_DATA)
                drDataFile = os.path.join(baseImagesFolder, drTag + OmeroConstants.FILE_OMERO_IMAGES_DATA)
                self.omeroFileService.updateDRData(drDataFile, newData)


def main(drTag, artefactsFolder, imagesFolder, logsFolder, omeroDevPropetiesFile, imagingCleanPath):
    t = time.time()
    tstamp = datetime.datetime.fromtimestamp(t).strftime('%Y%m%d')
    log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    logging.basicConfig(format=log_format, filename=logsFolder + drTag + '_' + tstamp + '.log', level=logging.INFO)

    preUploadCleanup = PreUploadCleanup(drTag, artefactsFolder, imagesFolder, imagingCleanPath)
    preUploadCleanup.createFoldersInClean()
    preUploadCleanup.cleanUpCSV()
    preUploadCleanup.moveDataAlreadyUploaded()

#    uploadCSVToOmero = UploadCSVToOmero(artefactsFolder, omeroDevPropetiesFile)
#    uploadCSVToOmero.prepareData(drTag, artefactsFolder, imagesFolder)
#    uploadCSVToOmero.doUpload(drTag, artefactsFolder, imagesFolder)
#    preUploadCleanup.moveDataAlreadyUploaded()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
