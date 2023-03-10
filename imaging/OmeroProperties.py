class OmeroProperties:
    omeroProperties = {}

    def __init__(self, omeroPropertiesFile):
        self.omeroProperties = {}
        with open(omeroPropertiesFile, 'r') as fh:
            lines = fh.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue

            segs = line.split('=')
            self.omeroProperties[segs[0].strip()] = segs[1].strip()

    def getProperties(self):
        return self.omeroProperties
