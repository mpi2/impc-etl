FILE_MEDIA_DATA_SUFFIX = '_media_data.json'
FILE_BASE_MEDIA = 'base' + FILE_MEDIA_DATA_SUFFIX
FILE_OMERO_IMAGE_DATA_LIST = 'image_data.list'
FILE_OMERO_IMAGES_DATA = '_imagedata.json'
FILE_OMERO_ANNOTATIONS_DATA = '_annotationslist.json'
FILE_OMERO_IMAGES_DATA_PREFIX = 'imagelist_'
FOLDER_OMERO_IMAGES_DATA = 'images_data'
FILE_MISSING_DATASOURCES = 'missing_datasources.list'

OMERO_DB_NAME = 'omero.omerodbname'
OMERO_DB_USER = 'omero.omerodbuser'
OMERO_DB_PASS = 'omero.omerodbpass'
OMERO_DB_HOST = 'omero.omerodbhost'
OMERO_DB_PORT = 'omero.omerodbport'

OMERO_APP_HOST = 'omero.omerohost'
OMERO_APP_PORT = 'omero.omeroport'
OMERO_APP_USER = 'omero.omerouser'
OMERO_APP_PASS = 'omero.omeropass'
OMERO_APP_GROUP = 'omero.omerogroup'

SITES = {
    'bcm': 'BCM',
    'gmc': 'HMGU',
    'h': 'MRC Harwell',
    'ics': 'ICS',
    'j': 'JAX',
    'tcp': 'TCP',
    'ning': 'NING',
    'rbrc': 'RBRC',
    'ucd': 'UC Davis',
    'wtsi': 'WTSI',
    'kmpc': 'KMPC',
    'ccpcz': 'CCP-IMG'
}

DATASOURCE_LIST = {
    1: 'BCM',
    2: 'HMGU',
    3: 'ICS',
    4: 'JAX',
    5: 'MRC Harwell',
    6: 'NING',
    7: 'RBRC',
    8: 'TCP',
    9: 'UC Davis',
    10: 'WTSI',
    11: 'JAX',
    51: 'MARC',
    101: 'KMPC',
    151: 'TCP',
    152: 'CCP-IMG',
    201: 'CCP-IMG',
    202: 'KMPC'
}

PREFERRED_DS_JAX = 4
PREFERRED_DS_TCP = 8
PREFERRED_DS_KMPC = 101
PREFERRED_DS_CCP_IMG = 152


def getParentDatasourceId(parent):
    if parent == 'JAX':
        return PREFERRED_DS_JAX
    if parent == 'TCP':
        return PREFERRED_DS_TCP
    if parent == 'KMPC':
        return PREFERRED_DS_KMPC
    if parent == 'CCP-IMG':
        return PREFERRED_DS_CCP_IMG

    for parentId in DATASOURCE_LIST:
        if DATASOURCE_LIST[parentId] == parent:
            return parentId

    return None
