import json
import sys


def main(jsonFile, logFile):
    with open(jsonFile, 'r') as fh:
        jsonData = json.load(fh)

    with open(logFile, 'r') as fh:
        lines = fh.readlines()

    total = 0
    success = 0
    failed = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        segs = line.split('::')
        total += 1

        if segs[2].strip() == 'Y':
            success += 1
        else:
            failed += 1

    print(' - Total data entries: {}'.format(len(jsonData)))
    print(' - Total downloaded so far: {} (Success: {} | Failed: {})'.format(total, success, failed))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
