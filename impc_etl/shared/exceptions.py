class UnsupportedEntityError(Exception):
    """Raise when the requested data entity type to extract is no supported by an extractor"""


class UnsupportedFileTypeError(Exception):
    """Raise when the requested data file type to extract is no supported by an extractor"""


class NoDataFoundError(Exception):
    """Raise when the requested data entity is not present on the files provided"""
