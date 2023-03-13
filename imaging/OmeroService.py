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

    def loadFileOrDir(self, directory, project=None, dataset=None, filenames=None):
        if filenames is not None:
            str_n_files = str(len(filenames))
        else:
            str_n_files = "0"

        self.logger.info(' -- Loading [' + directory + '] in [' + project + ' | ' + str(dataset) + ' ]: ' + str_n_files)
        # chop dir to get project and dataset

        # if filenames is non then load the entire dir
        if filenames is not None:
            for filename in filenames:
                fullPath = directory + "/" + filename
                self.logger.info(' --- Loading files: ' + fullPath)
        #                try:
        #                    self.load(fullPath, project, dataset)
        #                except Exception as e:
        #                    self.logger.warning("OmeroService Unexpected error loading file:" + str(e))
        #                    self.logger.warning("Skipping " + fullPath + " and continuing")
        #                    continue

        else:
            self.logger.info(' --- Loading directory: ' + directory)

    #            try:
    #                self.load(directory, project, dataset)
    #            except Exception as e:
    #                self.logger.exception("OmeroService Unexpected error loading directory:" + str(e))

    def load(self, path, project=None, dataset=None):
        self.logger.info("-" * 10)
        self.logger.info("path=" + path)
        self.logger.info("project=" + str(project))
        self.logger.info("dataset=" + str(dataset))
        # if self.cli is None or self.conn is None:
        # print "cli is none!!!!!"
        self.getConnection()

        import_args = ["import"]
        if project is not None:
            self.logger.info("project in load is not None. Project name: " + project)
            # project=project.replace(" ","-")
        if dataset is not None:
            self.logger.info("dataset in load is not None. Dataset name: " + dataset)

            ## Introducing a hack to see if the actual upload works
            dsId = self.dsData[dataset]
            self.logger.info("DatasetId (first try) =" + str(dsId))
            if not dsId:
                dsId = self.dsData[dataset.upper()]
                self.logger.info("DatasetId (second try) =" + str(dsId))
                if not dsId:
                    return

            self.logger.info("datasetId=" + str(dsId))
            import_args.extend(
                ["--", "-d", str(dsId), "--exclude", "filename"])  # "--no_thumbnails",,"--debug", "ALL"])
            # import_args.extend(["--","--transfer","ln_s","-d", str(dsId), "--exclude","filename"])#"--no_thumbnails",,"--debug", "ALL"])
            # import_args.extend(["--", "-d", str(dsId)])#"--no_thumbnails",,"--debug", "ALL"])
        else:
            self.logger.warning("dataset is None!!!!!!!!!!!!!!!!!!!!")

        self.logger.info('importing project=' + str(project) + ', dataset=' + str(dataset) + ', filename=' + str(path))

        if (path.endswith('.pdf')):
            self.logger.info(
                "We have a pdf document- loading as attachment " + str(path))  # we need to upload as an attachment
            namespace = "imperial.training.demo"
            fileAnn = self.conn.createFileAnnfromLocalFile(str(path), mimetype=None, ns=namespace, desc=None)
            self.logger.info("fileAnn=" + str(fileAnn))
            datasetForAnnotation = self.conn.getObject("Dataset", dsId)
            self.logger.info("Attaching FileAnnotation to Dataset: " + str(datasetForAnnotation) + ", File ID: " + str(
                fileAnn.getId()) + ", File Name: " + fileAnn.getFile().getName() + ", Size:" + str(
                fileAnn.getFile().getSize()))
            self.logger.info("Dataset=" + str(datasetForAnnotation))
            datasetForAnnotation.linkAnnotation(fileAnn)
            self.logger.info("linked annotation!")
        else:
            import_args.append(path)
            # print " import args="
            # print import_args
            self.cli.invoke(import_args, strict=True)
        self.conn._closeSession()
        # print "-" * 100
