import json
import os

import psycopg2

from imaging import OmeroConstants
from imaging.omero_util import retrieveDatasourcesFromDB


class OmeroFileService:
    omeroProperties = None
    dsList = None

    def __init__(self, omeroProperties):
        self.omeroProperties = omeroProperties
        self.dsList = self.consolidateDatasources()

    def consolidateDatasources(self):
        dsData = retrieveDatasourcesFromDB(self.omeroProperties)
        dsList = []
        for ds in dsData:
            dsList.append(dsData[ds])
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

    def retrieveAnnotationsFromOmero(self):
        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        fileData = []
        for ds in self.dsList:
            query = 'SELECT a.id,of.name,of.path FROM annotation a INNER JOIN datasetannotationlink dsal ON a.id=dsal.child INNER JOIN originalfile of ON a.file=of.id WHERE dsal.parent=' + str(
                ds)
            cur.execute(query)
            for (id, name, path) in cur.fetchall():
                fileData.append({
                    'id': id,
                    'name': name,
                    'path': path,
                    'type': 'annotation'
                })
        conn.close()
        return fileData

    def retrieveImagesFromOmeroAndSerialize(self, imagesDataFolder, filePrefix):
        if not os.path.exists(imagesDataFolder):
            os.mkdir(imagesDataFolder, 0o766)

        conn = psycopg2.connect(database=self.omeroProperties[OmeroConstants.OMERO_DB_NAME],
                                user=self.omeroProperties[OmeroConstants.OMERO_DB_USER],
                                password=self.omeroProperties[OmeroConstants.OMERO_DB_PASS],
                                host=self.omeroProperties[OmeroConstants.OMERO_DB_HOST],
                                port=self.omeroProperties[OmeroConstants.OMERO_DB_PORT])
        cur = conn.cursor()
        fileData = []
        count = 1
        masterCount = 1

        for ds in self.dsList:
            query = 'SELECT i.id,i.name,fse.clientpath FROM image i INNER JOIN datasetimagelink dsil ON i.id=dsil.child INNER JOIN filesetentry fse ON i.fileset=fse.fileset WHERE dsil.parent=' + str(
                ds)
            cur.execute(query)
            for (id, name, clientpath) in cur.fetchall():
                if count % 500000 == 0:
                    with open(os.path.join(imagesDataFolder, filePrefix + str(masterCount) + '.json'), 'w') as fh:
                        json.dump(fileData, fh, sort_keys=True, indent=4)
                    masterCount += 1
                    fileData = []

                count += 1
                fileData.append({
                    'id': id,
                    'name': name,
                    'path': clientpath,
                    'type': 'image'
                })
        conn.close()

        with open(os.path.join(imagesDataFolder, filePrefix + str(masterCount) + '.json'), 'w') as fh:
            json.dump(fileData, fh, sort_keys=True, indent=4)

    def processToList(self, fileData):
        fileList = []
        for el in fileData:
            if el['type'] == 'image':
                fileList.append(el['path'].split('impc/')[-1])
            else:
                fileList.append(el['path'].split('impc/')[-1] + '/' + el['name'])
        return fileList

    def checkImageDataOnDisk(self, imagesFolder):
        if os.path.exists(imagesFolder):
            noFiles = len(os.listdir(imagesFolder))
            return noFiles != 0
        else:
            return False
