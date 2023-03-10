import sys

import psycopg2

from imaging import OmeroConstants
from imaging.omero_util import retrieveDatasourcesFromDB
from imaging.read_omero_properties import OmeroProperties


def retrieveLEI_LIFIds(omeroProperties):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
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


def retrieveAnnotationsFromOmero(omeroProperties, dsList):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    cur = conn.cursor()
    fileData = {}
    for ds in dsList:
        query = 'SELECT a.id,of.name,of.path FROM annotation a INNER JOIN datasetannotationlink dsal ON a.id=dsal.child INNER JOIN originalfile of ON a.file=of.id WHERE dsal.parent=' + str(
            ds)
        cur.execute(query)
        for (id, name, path) in cur.fetchall():
            print(id + ' - ' + name + ' - ' + path)
    conn.close()


def retrieveFileListFromOmero(omeroProperties):
    dsList = consolidateDatasources(omeroProperties)
    retrieveAnnotationsFromOmero(omeroProperties, dsList)


def consolidateDatasources(omeroProperties):
    dsData = retrieveDatasourcesFromDB(omeroProperties)
    dsList = []
    for ds in dsData:
        dsList.append(dsData[ds])
    for ds in OmeroConstants.DATASOURCE_LIST:
        dsList.append(ds)
    return dsList


def main(omeroDevPropetiesFile):
    omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
    retrieveFileListFromOmero(omeroProperties)


if __name__ == "__main__":
    main(sys.argv[1])
