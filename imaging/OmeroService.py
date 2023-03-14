import logging

import omero
import omero.cli
from omero.gateway import *

from imaging import OmeroConstants
from imaging.omero_util import retrieveDatasourcesFromDB


class OmeroService:
    omeroProperties = None
    dsData = None

    def __init__(self, omeroProperties):
        self.logger = logging.getLogger(__name__)
        self.omeroProperties = omeroProperties
        self.dsData = retrieveDatasourcesFromDB(omeroProperties)

        self.conn = self.getConnection()

    def getConnection(self):
        try:
            self.logger.info('Initializing Omero connection ...')
            self.cli = omero.cli.CLI()
            self.cli.loadplugins()
            self.cli.invoke(["login", self.omeroProperties[OmeroConstants.OMERO_APP_USER] + '@' + self.omeroProperties[
                OmeroConstants.OMERO_APP_HOST], "-w", self.omeroProperties[OmeroConstants.OMERO_APP_PASS], "-C"],
                            strict=True)
            self.cli.invoke(["sessions", "group", self.omeroProperties[OmeroConstants.OMERO_APP_GROUP]], strict=True)
            sessionId = self.cli._event_context.sessionUuid
            self.conn = BlitzGateway(host=self.omeroProperties[OmeroConstants.OMERO_APP_HOST])
            self.conn.connect(sUuid=sessionId)
            self.logger.info('Omero connection initialized ...')
            return self.conn

        except Exception, e:
            self.logger.exception(e)

    def loadFileOrDir(self, directory, dataset=None, filenames=None):
        if filenames is not None:
            str_n_files = str(len(filenames))
        else:
            str_n_files = "0"

        self.logger.info(' -- Loading [' + directory + '] in [' + str(dataset) + ' ]: ' + str_n_files)
        # chop dir to get project and dataset

        # if filenames is non then load the entire dir
        if filenames is not None:
            for filename in filenames:
                fullPath = directory + "/" + filename
                self.logger.info(' --- Loading files: ' + fullPath)
                try:
                    self.load(fullPath, dataset)
                except Exception as e:
                    self.logger.error(' --- ERROR: Error loading file [' + fullPath + ']:' + str(e))
                    self.logger.error(' --- ERROR: Skipping file: ' + fullPath)
                    continue

        else:
            self.logger.info(' --- Loading directory: ' + directory)
            try:
                self.load(directory, dataset)
            except Exception as e:
                self.logger.error(' --- ERROR: Error loading directory [' + directory + ']:' + str(e))
                self.logger.error(' --- ERROR: Skipping: ' + directory)

    def load(self, path, dataset=None):
        self.getConnection()

        import_args = ["import"]
        if not dataset:
            self.logger.error(' -- ERROR: Dataset not provided!')
            return

        dsId = self.dsData[dataset]
        if not dsId:
            dsId = self.dsData[dataset.upper()]
            if not dsId:
                self.logger.error(' -- ERROR: Cannot find ID for dataset: ' + dataset)
                return

        import_args.extend(["--", "-d", str(dsId), "--exclude", "filename"])

        if (path.endswith('.pdf')):
            namespace = "imperial.training.demo"
            fileAnn = self.conn.createFileAnnfromLocalFile(str(path), mimetype=None, ns=namespace, desc=None)
            datasetForAnnotation = self.conn.getObject("Dataset", dsId)
            datasetForAnnotation.linkAnnotation(fileAnn)
            self.logger.info(' --- Loaded: ' + path)
        else:
            import_args.append(path)
            self.cli.invoke(import_args, strict=True)
            self.logger.info(' --- Loaded: ' + path)
        self.conn._closeSession()
