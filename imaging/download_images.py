import json
import os
import sys
from os.path import join

import requests

from imaging import OmeroConstants


def main(inFile, outFolder, outLog):
    with open(outLog, 'w') as logFh:
        with open(inFile, 'r') as fh:
            data = json.load(fh)
            count = 1
            for el in data:
                site = OmeroConstants.SITES[el['centre'].lower()]
                imgFolder = outFolder + site + '/' + el['pipelineKey'] + '/' + el['procedureKey'] + '/' + el[
                    'parameterKey']
                toDownload = el['dccUrl']
                fileName = str(el['id']) + '.' + el['extension']
                outFile = join(imgFolder, fileName)
                if os.path.isfile(outFile):
                    count += 1
                    continue

                response = requests.get(toDownload)
                if response.status_code == 200:
                    with open(outFile, 'wb') as outFileFh:
                        outFileFh.write(response.content)
                    logFh.write(OmeroConstants.SITES[el['centre'].lower()] + ' :: ' + el['checksum'] + ' :: Y\n')
                    logFh.flush()
                else:
                    logFh.write(OmeroConstants.SITES[el['centre'].lower()] + ' :: ' + el['checksum'] + ' :: N\n')
                    logFh.flush()

                count += 1


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
