import json
import os
import sys


class AddOmeroIdsToCSVFile:
    mediaData = {}
    omeroData = {}

    def __init__(self, mediaDataFolder, omeroDataFolder):
        self.mediaData = {}
        self.loadMediaData(mediaDataFolder)

        self.omeroData = {}
        self.loadOmeroData(omeroDataFolder)

    def loadMediaData(self, mediaDataFolder):
        print('Loading media data from: {}'.format(mediaDataFolder))
        for file in os.listdir(mediaDataFolder):
            with open(os.path.join(mediaDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)

            for el in jsonData:
                self.mediaData[el['checksum']] = el

    def loadOmeroData(self, omeroDataFolder):
        print('Loading existing omero data from: {}'.format(omeroDataFolder))
        for file in os.listdir(omeroDataFolder):
            with open(os.path.join(omeroDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)
                for el in jsonData:
                    folderPath = el['path'].split('impc/')[-1]
                    self.omeroData[folderPath.lower()] = el['id']

    def compileAndWriteToDisk(self, inCsvFile, outCsvFile):
        outLines = []

        notFound = 0
        totalEntries = 1
        with open(inCsvFile, 'r') as fh:
            lines = fh.readlines()

        for line in lines:
            line = line.strip()
            if line.startswith('observation_id'):
                outLines.append(line + ',omero_id,')
                continue

            totalEntries += 1
            segs = line.split(',')
            dccUrl = segs[2]
            if not dccUrl.startswith('https://'):
                outLines.append(line + ',-1,')
                notFound += 1
                continue

            cksum = dccUrl.split('getfile/')[-1]
            if not cksum in self.mediaData:
                outLines.append(line + ',-1,')
                notFound += 1
                continue

            element = self.mediaData[cksum]
            path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                'parameter'] + '/' + element['fileName']

            if path.lower() in self.omeroData:
                outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
            else:
                outLines.append(line + ',-1,')
                notFound += 1

        with open(outCsvFile, 'w') as fh:
            fh.write('\n'.join(outLines))

        print('Found {} missing image entries in the CSV [Total: {}].'.format(notFound, totalEntries))


def main(inCsvFile, outCsvFile, mediaDataFolder, omeroDataFolder):
    addOmeroIdsToCSVFile = AddOmeroIdsToCSVFile(mediaDataFolder, omeroDataFolder)
    addOmeroIdsToCSVFile.compileAndWriteToDisk(inCsvFile, outCsvFile)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
