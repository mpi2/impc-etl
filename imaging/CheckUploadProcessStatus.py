import os
import sys


class CheckUploadProcessStatus:
    artefactsFolder = None

    def __init__(self, artefactsFolder):
        self.artefactsFolder = artefactsFolder

    def checkIfProcessIsStillRunning(self):
        psOutput = os.popen('ps x | grep imaging').read()
        lines = psOutput.split('\n')

        psPid = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            segments = line.split(' ')
            actualSegments = []
            for seg in segments:
                seg = seg.strip()
                if seg:
                    actualSegments.append(seg)

            if not actualSegments:
                continue
            pid = actualSegments[0]
            for seg in actualSegments:
                if seg == 'imaging/upload_csv_to_omero.py':
                    psPid = pid
                    break
            if psPid:
                break

        return psPid


def main(artefactsFolder):
    print(' -- Using artefacts from: ' + artefactsFolder)

    checkUploadProcessStatus = CheckUploadProcessStatus(artefactsFolder)
    psPid = checkUploadProcessStatus.checkIfProcessIsStillRunning()
    if not psPid:
        print(' -- Upload process is no longer running!')
    else:
        print(' -- Upload process is still running. PID: ' + str(psPid))


if __name__ == "__main__":
    main(sys.argv[1])
