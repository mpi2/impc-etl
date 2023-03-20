import json
import os
import sys

import psycopg2

from imaging import OmeroConstants
from imaging.OmeroProperties import OmeroProperties


class RetrieveAndSerializeOmeroIds:
    omeroProperties = None
    outFolder = None
    drTag = None

    def __init__(self, omeroDevPropetiesFile, outFolder, drTag):
        self.omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
        self.outFolder = outFolder
        self.drTag = drTag

    def retrieveAnnotationsAndSerialize(self):
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
                clientPath = path
                if clientPath.startswith('/'):
                    clientPath = clientPath[1:]
                fileData.append({
                    'id': id,
                    'name': name,
                    'path': clientPath,
                    'type': 'annotation'
                })
        conn.close()
        with open(self.outFolder + self.drTag + '_annotations.json', 'w') as filehandle:
            json.dump(fileData, filehandle, sort_keys=True, indent=4)

    def retrieveImagesAndSerialize(self):
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
                    with open(os.path.join(self.outFolder, self.drTag + '_' + str(masterCount) + '.json'), 'w') as fh:
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

        with open(os.path.join(self.outFolder, self.drTag + '_' + str(masterCount) + '.json'), 'w') as fh:
            json.dump(fileData, fh, sort_keys=True, indent=4)


def main(omeroDevPropetiesFile, outFolder, drTag):
    retrieveAndSerializeOmeroIds = RetrieveAndSerializeOmeroIds(omeroDevPropetiesFile, outFolder, drTag)
    retrieveAndSerializeOmeroIds.retrieveAnnotationsAndSerialize()
    retrieveAndSerializeOmeroIds.retrieveImagesAndSerialize()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
