import json
import os

import psycopg2

from imaging import OmeroConstants, OmeroUtil


class OmeroFileService:
    omeroProperties = None
    dsList = None

    def __init__(self, omeroProperties):
        self.omeroProperties = omeroProperties
        self.dsList = self.consolidateDatasources()

    def consolidateDatasources(self):
        dsData = OmeroUtil.retrieveDatasourcesFromDB(self.omeroProperties)
        dsList = []
        for dsId in dsData:
            dsList.append(dsId)
        for ds in OmeroConstants.DATASOURCE_LIST:
            dsList.append(ds)
        return dsList

    def retrieveLEI_LIFIds(self):
        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        fileData = {}

        query = "SELECT DISTINCT i.id, i.name FROM image i INNER JOIN datasetimagelink dsil ON i.id=dsil.child INNER JOIN filesetentry fse ON i.fileset=fse.fileset WHERE LOWER(i.name) LIKE '%lif%'"
        cur.execute(query)
        for (id, name) in cur.fetchall():
            fileData[id] = name

        query = "SELECT DISTINCT i.id, i.name FROM image i INNER JOIN datasetimagelink dsil ON i.id=dsil.child INNER JOIN filesetentry fse ON i.fileset=fse.fileset WHERE LOWER(i.name) LIKE '%lei%'"
        cur.execute(query)
        for (id, name) in cur.fetchall():
            fileData[id] = name

        conn.close()
        return fileData

    def runUpdate(self, drTag):
        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        query = "BEGIN; UPDATE filesetentry SET clientpath=REPLACE(clientpath,'holding_area/impc/" + drTag + "/images','clean/impc') WHERE clientpath LIKE '%holding_area/impc/" + drTag + "%'; UPDATE originalfile SET path=REPLACE(path,'holding_area/impc/" + drTag + "/images','clean/impc') WHERE path LIKE '%holding_area/impc/" + drTag + "%'; COMMIT;"
        cur.execute(query)
        conn.close()

    def checkImageDataOnDisk(self, imageDataFolder, imageDataListFile):
        if os.path.exists(imageDataFolder):
            noFiles = len(os.listdir(imageDataFolder))
            return noFiles != 0 and os.path.exists(imageDataListFile)
        else:
            return False

    def loadImageDataFromFile(self, imageDataFile):
        fileList = []
        with open(imageDataFile, 'r') as fh:
            lines = fh.readlines()
        for line in lines:
            line = line.strip()
            if line:
                fileList.append(line)
        return fileList

    def retrieveImagesFromOmero(self, drTag):
        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        fileData = []
        query = "SELECT i.id,i.name,fse.clientpath FROM image i INNER JOIN datasetimagelink dsil ON i.id=dsil.child INNER JOIN filesetentry fse ON i.fileset=fse.fileset WHERE fse.clientpath LIKE '%holding_area/impc/" + drTag + "%';"
        cur.execute(query)
        for (id, name, clientpath) in cur.fetchall():
            fileData.append({
                'id': id,
                'name': name,
                'path': clientpath,
                'type': 'image'
            })
        conn.close()
        return fileData

    def retrieveAnnotationsFromOmero(self, drTag):
        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        fileData = []
        query = "SELECT a.id,of.name,of.path FROM annotation a INNER JOIN datasetannotationlink dsal ON a.id=dsal.child INNER JOIN originalfile of ON a.file=of.id WHERE of.path LIKE '%holding_area/impc/" + drTag + "%';"
        cur.execute(query)
        for (id, name, path) in cur.fetchall():
            clientPath = path
            if clientPath.startswith('/'):
                clientPath = clientPath[1:]
            fileData.append({
                'id': id,
                'name': name,
                'path': clientPath + '/' + name,
                'type': 'annotation'
            })
        conn.close()
        return fileData

    def updateDRData(self, drDataFile, newData):
        drData = []
        if os.path.exists(drDataFile):
            with open(drDataFile, 'r') as fh:
                drData = json.load(fh)
        drData.extend(newData)
        if os.path.exists(drDataFile):
            os.remove(drDataFile)
        with open(drDataFile, 'w') as filehandle:
            json.dump(drData, filehandle, sort_keys=True, indent=4)
