import json
import sys

from imaging import OmeroUtil
from imaging.OmeroProperties import OmeroProperties


class RetrieveDatasourcesFromOmero:
    omeroProperties = None

    def __init__(self, omeroDevPropetiesFile):
        self.omeroProperties = OmeroProperties(omeroDevPropetiesFile)

    def retrieveDatasources(self, outFile):
        dsData = OmeroUtil.retrieveDatasourcesFromDB(self.omeroProperties.getProperties())
        with open(outFile, 'w') as fh:
            json.dump(dsData, fh, sort_keys=True, indent=4)


def main(omeroDevPropetiesFile, outFile):
    retrieveDatasourcesFromOmero = RetrieveDatasourcesFromOmero(omeroDevPropetiesFile)
    retrieveDatasourcesFromOmero.retrieveDatasources(outFile)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
