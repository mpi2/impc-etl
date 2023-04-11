import json
import os


class AddOmeroIdsToCSVFile:
    mediaData = {}
    omeroData = {}

    def __init__(self, mediaDataFolder, omeroDataFolder):
        self.mediaData = {}
        self.loadMediaData(mediaDataFolder)

        self.omeroData = {}
        self.loadOmeroData(omeroDataFolder)

    def loadMediaData(self, mediaDataFolder):
        print('Loading media data from: ' + mediaDataFolder)
        for file in os.listdir(mediaDataFolder):
            with open(os.path.join(mediaDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)

            for el in jsonData:
                self.mediaData[el['checksum']] = el

    def loadOmeroData(self, omeroDataFolder):
        print('Loading existing omero data from: ' + omeroDataFolder)
        for file in os.listdir(omeroDataFolder):
            with open(os.path.join(omeroDataFolder, file), 'r') as fh:
                jsonData = json.load(fh)
                for el in jsonData:
                    folderPath = el['path'].split('impc/')[-1]
                    self.omeroData[folderPath.lower()] = el['id']

    def compileAndWriteToDisk(self, inCsvFile, outCsvFile, notFoundFile):
        outLines = []
        undesired_extensions = ['mov', 'bin', 'fcs', 'nrrd', 'bz2', 'arf', 'qt']
        target_extensions = ['jpeg', 'jpg', 'tiff', 'tif']

        notFound = 0
        notFoundList = []
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
                continue

            cksum = dccUrl.split('getfile/')[-1]
            if not cksum in self.mediaData:
                outLines.append(line + ',-1,')
                notFound += 1
                continue

            element = self.mediaData[cksum]
            fileNameExt = element['fileName'][element['fileName'].rfind('.') + 1:]
            if fileNameExt in undesired_extensions:
                outLines.append(line + ',-1,')
                continue

            path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                'parameter'] + '/' + element['fileName']

            if path.lower() in self.omeroData:
                outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
            else:
                if not fileNameExt in target_extensions:
                    outLines.append(line + ',-1,')
                    notFound += 1
                    notFoundList.append(cksum + ' << ' + path)
                    continue

                fileFound = True
                if fileNameExt == 'jpeg':
                    path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                        'parameter'] + '/' + element['fileName'].replace('jpeg', 'jpg')
                    if path.lower() in self.omeroData:
                        outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
                    else:
                        fileFound = False
                if fileNameExt == 'jpg':
                    path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                        'parameter'] + '/' + element['fileName'].replace('jpg', 'jpeg')
                    if path.lower() in self.omeroData:
                        outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
                    else:
                        fileFound = False
                if fileNameExt == 'tiff':
                    path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                        'parameter'] + '/' + element['fileName'].replace('tiff', 'tif')
                    if path.lower() in self.omeroData:
                        outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
                    else:
                        fileFound = False
                if fileNameExt == 'tif':
                    path = element['centre'] + '/' + element['pipeline'] + '/' + element['procedure'] + '/' + element[
                        'parameter'] + '/' + element['fileName'].replace('tif', 'tiff')
                    if path.lower() in self.omeroData:
                        outLines.append(line + ',' + str(self.omeroData[path.lower()]) + ',')
                    else:
                        fileFound = False
                if not fileFound:
                    outLines.append(line + ',-1,')
                    notFound += 1
                    notFoundList.append(cksum + ' << ' + path)
                    continue

        with open(outCsvFile, 'w') as fh:
            fh.write('\n'.join(outLines))

        with open(notFoundFile, 'w') as fh:
            fh.write('\n'.join(notFoundList))

        print('Found ' + str(notFound) + ' missing image entries in the CSV [Total: ' + str(totalEntries) + '].')


def main(inCsvFile, outCsvFile, notFoundFile, mediaDataFolder, omeroDataFolder):
    addOmeroIdsToCSVFile = AddOmeroIdsToCSVFile(mediaDataFolder, omeroDataFolder)
    addOmeroIdsToCSVFile.compileAndWriteToDisk(inCsvFile, outCsvFile, notFoundFile)


if __name__ == '__main__':
    #    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    main('/home/tudor/__data__/dr19/omero_ids_population/impc_images_input_wo_omero_ids.csv',
         '/home/tudor/__data__/dr19/omero_ids_population/out.csv',
         '/home/tudor/__data__/dr19/omero_ids_population/not_found.list',
         '/home/tudor/__data__/dr19/omero_ids_population/media_data/',
         '/home/tudor/__data__/dr19/omero_ids_population/images_data/')
