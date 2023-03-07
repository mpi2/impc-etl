import json
import os
import sys

import requests


def main(targetDate, outFolder):
    done = False
    offset = 0
    total = 0

    all_data = []

    print('- Calling media API to retrieve latest set of entries since: {}'.format(targetDate))
    while not done:
        query_string = 'https://api.mousephenotype.org/media/updatedsince/' + targetDate + '?limit=10000&offset=' + str(
            offset * 10000)
        v = json.loads(requests.get(query_string).text)
        all_data.extend(v)
        total += len(v)
        if len(v) == 0:
            done = True
        offset += 1

    print('- Retrieved {} entries ...'.format(total))

    if os.path.isfile(outFolder + 'data.json'):
        os.remove(outFolder + 'data.json')

    with open(outFolder + 'data.json', 'w') as filehandle:
        json.dump(all_data, filehandle, sort_keys=True, indent=4)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
