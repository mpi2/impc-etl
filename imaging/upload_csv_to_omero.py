import csv
import datetime
import glob
import logging
import os
import os.path
import sys
import time

from imaging import OmeroConstants
from imaging.OmeroFileService import OmeroFileService
from imaging.OmeroProperties import OmeroProperties
from imaging.OmeroService import OmeroService
from imaging.omero_util import writeImageDataToDiskInSegments, writeImageDataToDiskAsFile


def add_to_list(L, dirname, names):
    for n in names:
        fullname = os.path.join(dirname, n)
        if os.path.isfile(fullname):
            L.append(fullname)


def main(drTag, artefactsFolder, imagesFolder, logsFolder, omeroDevPropetiesFile):
    csvFile = artefactsFolder + drTag + '.csv'
    missingDSFile = artefactsFolder + OmeroConstants.FILE_MISSING_DATASOURCES

    if os.path.isfile(missingDSFile):
        print('ERROR: Unable to start the upload process as there still are missing datasources: ' + missingDSFile)
        sys.exit(-1)

    t = time.time()
    tstamp = datetime.datetime.fromtimestamp(t).strftime('%Y%m%d')
    log_format = '%(asctime)s - %(name)s - %(levelname)s:%(message)s'
    logging.basicConfig(format=log_format, filename=logsFolder + drTag + '_' + tstamp + '.log', level=logging.INFO)

    log_formatter = logging.Formatter(log_format)
    logger = logging.getLogger('OmeroUploadMainMethod')
    #    root_logger = logging.getLogger()

    #    console_handler = logging.StreamHandler()
    #    console_handler.setFormatter(log_formatter)
    #    root_logger.addHandler(console_handler)

    # Other values needed within this script.
    splitString = 'impc/'
    # Upload whole dir if it contains more than this number of files
    load_whole_dir_threshold = 300

    logger.info('Using CSV file for Omero upload: ' + csvFile)

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
            print
            "Fatal Error:"
            print
            str(e), header_row
            print
            "Exiting"
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
                print
                "Did not receive a required field - " + \
                "phenotyping_center='" + phenotyping_center + \
                "', pipeline_stable_id='" + pipeline_stable_id + \
                "', procedure_stable_id='" + procedure_stable_id + \
                "', parameter_stable_id='" + parameter_stable_id + \
                "' - not uploading: " + download_file_path
                continue
            fname = os.path.split(download_file_path)[-1]
            key = os.path.join(phenotyping_center, pipeline_stable_id, procedure_stable_id, parameter_stable_id, fname)
            csv_directory_to_filenames_map[key] = download_file_path
            n_from_csv_file += 1

    logger.info('Found ' + str(n_from_csv_file) + ' records to be uploaded to Omero.')

    omeroProperties = OmeroProperties(omeroDevPropetiesFile)
    omeroFileService = OmeroFileService(omeroProperties.getProperties())
    omeroService = OmeroService(omeroProperties.getProperties())

    imageDataExists = omeroFileService.checkImageDataOnDisk(
        artefactsFolder + drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX)
    logger.info('Omero image data on disk: ' + str(imageDataExists))
    if not imageDataExists:
        logger.info('Retrieving image list from Omero and serialising it on disk ...')
        omeroFileService.retrieveImagesFromOmeroAndSerialize(
            artefactsFolder + drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX,
            OmeroConstants.FILE_OMERO_IMAGES_DATA_PREFIX)

    logger.info('Retrieving annotations list from Omero ...')
    annotationFileData = omeroFileService.retrieveAnnotationsFromOmero()
    writeImageDataToDiskAsFile(artefactsFolder + drTag + OmeroConstants.FILE_OMERO_ANNOTATIONS_DATA, annotationFileData)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
