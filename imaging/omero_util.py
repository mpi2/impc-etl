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
