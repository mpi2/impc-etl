import json
import os.path
import sys


class CreateCSVForUploadToOmero:
    mediaData = {}
    availableImages = []

    def __init__(self, imagesFolder, jsonDataFile):
        self.mediaData = {}
        self.availableImages = []

        self.loadMediaData(jsonDataFile)
        self.loadAvailableImages(imagesFolder)

    def loadMediaData(self, jsonDataFile):
        print('Loading media data ...')
        with open(jsonDataFile, 'r') as fh:
            jsonData = json.load(fh)

        for el in jsonData:
            self.mediaData[el['checksum']] = el

    def loadAvailableImages(self, imagesFolder):
        print('Reading files available on disk ...')
        for centre in os.listdir(imagesFolder):
            centreFolder = os.path.join(imagesFolder, centre)

            for pipeline in os.listdir(centreFolder):
                pipelineFolder = os.path.join(centreFolder, pipeline)

                for procedure in os.listdir(pipelineFolder):
                    procedureFolder = os.path.join(pipelineFolder, procedure)

                    for parameter in os.listdir(procedureFolder):
                        parameterFolder = os.path.join(procedureFolder, parameter)

                        for imageFile in os.listdir(parameterFolder):
                            self.availableImages.append({
                                'centre': centre,
                                'pipeline': pipeline,
                                'procedure': procedure,
                                'parameter': parameter,
                                'fileName': imageFile
                            })

    def findChecksum(self, entry):
        for checksum in self.mediaData:
            el = self.mediaData[checksum]
            if el['centre'] == entry['centre'] and el['pipeline'] == entry['pipeline'] and el['procedure'] == entry[
                'procedure'] and el['parameter'] == entry['parameter'] and el['fileName'] == entry['fileName']:
                return checksum

        return None

    def createCSV(self, drTag, outputFolder):
        lines = []
        lines.append(
            'observation_id,increment_value,download_file_path,phenotyping_center,pipeline_stable_id,procedure_stable_id,datasource_name,parameter_stable_id')

        for entry in self.availableImages:
            checkSum = self.findChecksum(entry)
            if not checkSum:
                print('ERROR: Unable to find checksum for: {} - {} - {} - {} - {}'.format(entry['centre'],
                                                                                          entry['pipeline'],
                                                                                          entry['procedure'],
                                                                                          entry['parameter'],
                                                                                          entry['fileName']))
                continue
            newLine = 'xxx,xxx,'
            newLine += 'https://api.mousephenotype.org/' + checkSum + '/' + entry['fileName'] + ','
            newLine += entry['centre'] + ',' + entry['pipeline'] + ',' + entry['procedure'] + ',IMPC,' + entry[
                'parameter']
            lines.append(newLine)

        with open(outputFolder + drTag + '.csv', 'w') as fh:
            fh.write('\n'.join(lines))
        print(' - File created: ' + outputFolder + drTag + '.csv')


def main(imagesFolder, jsonDataFile, drTag, outputFolder):
    print('Creating CSV file to upload images to Omero ...')
    createCSVForUploadToOmero = CreateCSVForUploadToOmero(imagesFolder, jsonDataFile)
    createCSVForUploadToOmero.createCSV(drTag, outputFolder)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
