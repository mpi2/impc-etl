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

    logger.info('Retrieving image list from Omero ...')
    imageFileData = omeroFileService.retrieveImagesFromOmero()
    writeImageDataToDiskInSegments(artefactsFolder + drTag + OmeroConstants.FOLDER_OMERO_IMAGES_DATA_SUFFIX,
                                   OmeroConstants.FILE_OMERO_IMAGES_DATA_PREFIX, imageFileData)
    omero_file_list = omeroFileService.processToList(imageFileData)
    logger.info('Found ' + str(len(omero_file_list)) + ' images in Omero.')

    logger.info('Retrieving annotations list from Omero ...')
    annotationFileData = omeroFileService.retrieveAnnotationsFromOmero()
    writeImageDataToDiskAsFile(artefactsFolder + drTag + OmeroConstants.FILE_OMERO_ANNOTATIONS_DATA, annotationFileData)
    omero_annotation_list = omeroFileService.processToList(annotationFileData)
    logger.info('Found ' + str(len(omero_annotation_list)) + ' annotations in Omero.')

    omero_file_list.extend(omero_annotation_list)
    omero_dir_list = list(set([os.path.split(f)[0] for f in omero_file_list]))

    # Get the files in NFS
    list_nfs_filenames = []
    logger.info('Retrieving list of files on disk ...')
    os.path.walk(imagesFolder, add_to_list, list_nfs_filenames)
    list_nfs_filenames = [f.split(imagesFolder)[-1] for f in list_nfs_filenames]
    logger.info('Found ' + str(len(list_nfs_filenames)) + ' files on disk.')

    # Modified to carry out case-insensitive comparisons.
    set_csv_filenames = set([k.lower() for k in csv_directory_to_filenames_map.keys()])
    set_omero_filenames = set([f.lower() for f in omero_file_list])
    dict_nfs_filenames = {}
    for f in list_nfs_filenames:
        # Note that if more than one file maps to the same case insensitive value
        # only the last one encountered will be used
        dict_nfs_filenames[f.lower()] = f
    set_nfs_filenames = set(dict_nfs_filenames.keys())

    files_to_upload = set_csv_filenames - set_omero_filenames
    files_to_upload_available = files_to_upload.intersection(set_nfs_filenames)
    files_to_upload_unavailable = files_to_upload - files_to_upload_available

    logger.info('Number of files to upload: ' + str(len(files_to_upload)))
    logger.info('Number of files to available: ' + str(len(files_to_upload_available)))

    # Create a dictionary for the files to upload with the directory as the
    # key and the original nfs filenames as the values, so each dir can be passed to
    # omero with associated files
    dict_files_to_upload = {}
    for f in files_to_upload_available:
        dirname, filename = os.path.split(dict_nfs_filenames[f])
        if dict_files_to_upload.has_key(dirname):
            dict_files_to_upload[dirname].append(filename)
        else:
            dict_files_to_upload[dirname] = [filename]

    # Upload files
    n_dirs_to_upload = len(dict_files_to_upload)
    for index, directory in zip(range(n_dirs_to_upload), dict_files_to_upload.keys()):
        filenames = dict_files_to_upload[directory]
        n_files_to_upload = len(filenames)
        logger.info('Uploading: [' + str(index + 1) + ' of ' + str(n_dirs_to_upload) + ']: ' + directory)
        dir_structure = directory.split('/')
        project = dir_structure[0]
        # Below we assume dir_structure is list with elements:
        # [project, pipeline, procedure, parameter]
        dataset = "-".join(dir_structure)
        fullpath = os.path.join(imagesFolder, directory)

        # if dir contains pdf file we cannot load whole directory
        if len(glob.glob(os.path.join(fullpath, '*.pdf'))) > 0:
            logger.info(' -- Uploading PDFs ...')
            omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=filenames)
        else:
            # Check if the dir is in omero.
            # If not we can import the whole dir irrespective of number of files
            dir_not_in_omero = True
            try:
                if omero_dir_list.index(directory) >= 0:
                    dir_not_in_omero = False
            except ValueError:
                pass
            logger.info(' -- Uploading images ...')
            if dir_not_in_omero or n_files_to_upload > load_whole_dir_threshold:
                omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=None)
            else:
                omeroService.loadFileOrDir(fullpath, dataset=dataset, filenames=filenames)

    n_files_to_upload_unavailable = len(files_to_upload_unavailable)
    logger.warning('Number of files unavailable for upload: ' + str(n_files_to_upload_unavailable))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
