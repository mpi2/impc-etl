import json
import os
import sys


def main(jsonFile, logFile):
    print(' - Checking data download status ...')
    print(' - Using JSON file: {}'.format(jsonFile))
    print(' - Using LOG file: {}'.format(logFile))

    if not os.path.isfile(jsonFile):
        print(' -- ERROR: Unable to find JSON file: {}'.format(jsonFile))
        exit(-1)

    if not os.path.isfile(logFile):
        print(' -- ERROR: Unable to find LOG file: {}'.format(logFile))
        exit(-1)

    with open(jsonFile, 'r') as fh:
        jsonData = json.load(fh)

    with open(logFile, 'r') as fh:
        lines = fh.readlines()

    total = 0
    success = 0
    failed = 0
    failedData = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        segs = line.split('::')
        total += 1

        if segs[2].strip() == 'Y':
            success += 1
        else:
            center = segs[0].strip()
            checksum = segs[1].strip()
            lst = []
            if center in failedData:
                lst = failedData[center]
            lst.append(checksum)
            failedData[center] = lst
            failed += 1

    print(' - Total data entries: {}'.format(len(jsonData)))
    print(' - Total downloaded so far: {} (Success: {} | Failed: {})'.format(total, success, failed))
    if len(jsonData) == total:
        print(' - Download completed!')
        if failed != 0:
            for center in failedData:
                print(' -- Center: {}'.format(center))
                for el in failedData[center]:
                    print(' --- {}'.format(el))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
