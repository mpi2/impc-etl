import json
import os
import sys
from datetime import datetime

import requests

import OmeroConstants


class RetrieveMediaUpdates:
    all_media_data = {}
    new_media_data = {}
    existingMediaFile = None

    def __init__(self, drTag, mediaDataFolder):
        self.all_media_data = {}
        self.new_media_data = {}
        self.existingMediaFile = os.path.join(mediaDataFolder, drTag + OmeroConstants.FILE_MEDIA_DATA_SUFFIX)

        self.loadExistingData(mediaDataFolder)

    def loadExistingData(self, mediaDataFolder):
        for file in os.listdir(mediaDataFolder):
            with open(os.path.join(mediaDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)
            for el in jsonData:
                self.all_media_data[el['checksum']] = el

    def retrieveMediaData(self):
        done = False
        offset = 0
        total = 0

        thisYear = datetime.today().strftime('%Y')
        targetDate = str(int(thisYear) - 1) + '-07-01'
        print('- Calling media API to retrieve latest set of entries since: {}'.format(targetDate))
        while not done:
            query_string = 'https://api.mousephenotype.org/media/updatedsince/' + targetDate + '?limit=10000&offset=' + str(
                offset * 10000)
            v = json.loads(requests.get(query_string).text)
            for el in v:
                newEl = {
                    'centre': OmeroConstants.SITES[el['centre'].lower()],
                    'dccUrl': el['dccUrl'],
                    'checksum': el['checksum'],
                    'pipeline': el['pipelineKey'],
                    'procedure': el['procedureKey'],
                    'parameter': el['parameterKey'],
                    'fileName': str(el['id']) + '.' + el['extension']
                }
                if not newEl['checksum'] in self.all_media_data:
                    self.new_media_data[newEl['checksum']] = newEl
                    total += 1

            if len(v) == 0:
                done = True
            offset += 1
        print('- Retrieved {} new entries ...'.format(total))

    def getNewMediaData(self):
        return self.new_media_data

    def writeToDisk(self, mediaData=None):
        existingData = []

        if os.path.exists(self.existingMediaFile):
            with open(self.existingMediaFile, 'r') as fh:
                existingData = json.load(fh)

        if mediaData:
            existingData.extend(mediaData)
        else:
            to_write = []
            for cksum in self.new_media_data:
                to_write.append(self.new_media_data[cksum])
            existingData.extend(to_write)
        with open(self.existingMediaFile, 'w') as filehandle:
            json.dump(existingData, filehandle, sort_keys=True, indent=4)


def main(drTag, inOutFolder):
    retrieveMediaUpdates = RetrieveMediaUpdates(drTag, inOutFolder)
    retrieveMediaUpdates.retrieveMediaData()
    retrieveMediaUpdates.writeToDisk()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
