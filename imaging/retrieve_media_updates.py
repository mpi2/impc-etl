import json
import os
import sys
from datetime import datetime

import requests

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


def main(drTag, inOutFolder):
    done = False
    offset = 0
    total = 0

    all_media_data = {}
    for file in os.listdir(inOutFolder):
        if file != drTag + '_media_data.json':
            with open(os.path.join(inOutFolder, file), 'r') as fh:
                jsonData = json.load(fh)
            for el in jsonData:
                all_media_data[el['checksum']] = el

    existing_media_data = {}
    if os.path.exists(os.path.join(inOutFolder, drTag + '_media_data.json')):
        with open(os.path.join(inOutFolder, drTag + '_media_data.json'), 'r') as fh:
            jsonData = json.load(fh)
        for el in jsonData:
            existing_media_data[el['checksum']] = el

    thisYear = datetime.today().strftime('%Y')
    targetDate = str(int(thisYear) - 1) + '-07-01'
    print('- Calling media API to retrieve latest set of entries since: {}'.format(targetDate))
    while not done:
        query_string = 'https://api.mousephenotype.org/media/updatedsince/' + targetDate + '?limit=10000&offset=' + str(
            offset * 10000)
        v = json.loads(requests.get(query_string).text)
        for el in v:
            newEl = {
                'centre': SITES[el['centre'].lower()],
                'checksum': el['checksum'],
                'pipeline': el['pipelineKey'],
                'procedure': el['procedureKey'],
                'parameter': el['parameterKey'],
                'fileName': str(el['id']) + '.' + el['extension']
            }
            if not newEl['checksum'] in all_media_data:
                if not newEl['checksum'] in existing_media_data:
                    existing_media_data[newEl['checksum']] = newEl
                    total += 1

        if len(v) == 0:
            done = True
        offset += 1

    print('- Retrieved {} new entries ...'.format(total))
    to_write = []
    for cksum in existing_media_data:
        to_write.append(existing_media_data[cksum])
    with open(os.path.join(inOutFolder, drTag + '_media_data.json'), 'w') as filehandle:
        json.dump(to_write, filehandle, sort_keys=True, indent=4)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
