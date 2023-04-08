import json
import os
import sys

from .RetrieveMediaUpdates import RetrieveMediaUpdates


class CheckForMissingImagesInPipelineCSV:
    mediaData = {}
    missingData = []
    retrieveMediaUpdates = None

    def __init__(self, drTag, mediaDataFolder):
        self.mediaData = {}
        self.missingData = []
        self.retrieveMediaUpdates = RetrieveMediaUpdates(drTag, mediaDataFolder)

        self.loadMediaData(mediaDataFolder)

    def loadMediaData(self, mediaDataFolder):
        for file in os.listdir(mediaDataFolder):
            with open(os.path.join(mediaDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)

            for el in jsonData:
                self.mediaData[el['checksum']] = el

    def findMissingImages(self, csvFile):
        with open(csvFile, 'r') as fh:
            lines = fh.readlines()

        for line in lines:
            line = line.strip()
            if line.startswith('observation_id'):
                continue

            segs = line.split(',')
            dccUrl = segs[2]
            if not dccUrl.startswith('https://'):
                continue
            cksum = dccUrl.split('getfile/')[-1]
            if cksum in self.mediaData:
                continue

            site = segs[3]
            pipelineKey = segs[4]
            procedureKey = segs[5]
            parameterKey = segs[7]
            self.missingData.append({
                'centre': site,
                'checksum': cksum,
                'pipeline': pipelineKey,
                'procedure': procedureKey,
                'parameter': parameterKey
            })
        print('Found {} missing image entries in the CSV.'.format(len(self.missingData)))

    def consolidateMissingData(self):
        if len(self.missingData) == 0:
            print('All good. Please run the <Add Omero IDs to CSV> step.')
            return

        self.retrieveMediaUpdates.retrieveMediaData()
        latestMediaData = self.retrieveMediaUpdates.getNewMediaData()

        keepInMediaData = []
        for el in self.missingData:
            if el['checksum'] in latestMediaData:
                mediaDataEntry = latestMediaData[el['checksum']]

                if el['centre'] == mediaDataEntry['centre'] and el['pipeline'] == mediaDataEntry['pipeline'] and el[
                    'procedure'] == mediaDataEntry['procedure'] and el['parameter'] == mediaDataEntry['parameter']:
                    keepInMediaData.append(mediaDataEntry)
                else:
                    print('ERROR: CSV entry does not match entry found at DCC: {} - {} - {} - {} :: {}'.format(
                        el['centre'],
                        el['pipeline'],
                        el['procedure'],
                        el['parameter'],
                        el['checksum']))

            else:
                print('ERROR: CSV entry not found at DCC: {} - {} - {} - {} :: {}'.format(el['centre'], el['pipeline'],
                                                                                          el['procedure'],
                                                                                          el['parameter'],
                                                                                          el['checksum']))
        print('Found {} entries to download.'.format(len(keepInMediaData)))
        print(
            'Please run again the <Download data> and <Upload to Omero> steps and then skip to <Add Omero IDs to CSV>.')
        self.retrieveMediaUpdates.writeToDisk(mediaData=keepInMediaData)


def main(drTag, csvFile, mediaDataFolder):
    checkForMissingImagesInPipelineCSV = CheckForMissingImagesInPipelineCSV(drTag, mediaDataFolder)
    checkForMissingImagesInPipelineCSV.findMissingImages(csvFile)
    checkForMissingImagesInPipelineCSV.consolidateMissingData()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
