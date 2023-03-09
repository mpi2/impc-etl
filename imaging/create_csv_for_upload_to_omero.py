import json
import os.path
import sys

from imaging import OmeroConstants


def main(imagesFolder, jsonDataFile, drTag, outputFolder):
    with open(jsonDataFile, 'r') as fh:
        jsonData = json.load(fh)

    lines = []
    lines.append(
        'observation_id,increment_value,download_file_path,phenotyping_center,pipeline_stable_id,procedure_stable_id,datasource_name,parameter_stable_id')

    for el in jsonData:
        site = OmeroConstants.SITES[el['centre'].lower()]
        imgFile = imagesFolder + site + '/' + el['pipelineKey'] + '/' + el['procedureKey'] + '/' + el[
            'parameterKey'] + '/' + str(el['id']) + '.' + el['extension']
        if os.path.isfile(imgFile):
            newLine = 'xxx,xxx,'
            newLine += 'https://api.mousephenotype.org/' + el['checksum'] + '/' + str(el['id']) + '.' + el[
                'extension'] + ','
            newLine += site + ',' + el['pipelineKey'] + ',' + el['procedureKey'] + ',IMPC,' + el['parameterKey']
            lines.append(newLine)

    with open(outputFolder + drTag + '.csv', 'w') as fh:
        fh.write('\n'.join(lines))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
