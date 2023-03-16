import json
import os.path
import sys

import psycopg2

from imaging import OmeroConstants


def retrieveDatasourcesFromDB(omeroProperties):
    dsData = {}
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    for dsId in OmeroConstants.DATASOURCE_LIST:
        cur = conn.cursor()
        query = 'SELECT ds.id, ds.name FROM dataset ds INNER JOIN projectdatasetlink pdsl ON ds.id=pdsl.child WHERE pdsl.parent=' + str(
            dsId)
        cur.execute(query)
        for (id, name) in cur.fetchall():
            dsData[name] = int(id)
    conn.close()
    return dsData


def writeImageDataToDiskAsFile(fileOut, imageData):
    if os.path.isfile(fileOut):
        os.remove(fileOut)

    with open(fileOut, 'w') as filehandle:
        json.dump(imageData, filehandle, sort_keys=True, indent=4)


def loadDataFromFile(dataFile):
    with open(dataFile, 'r') as fh:
        fileData = json.load(fh)
    return fileData


def main(omeroProperties, outFile):
    dsData = retrieveDatasourcesFromDB(omeroProperties)
    with open(outFile, 'w') as fh:
        json.dump(dsData, fh, sort_keys=True, indent=4)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
