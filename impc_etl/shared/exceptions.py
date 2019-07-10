class UnsupportedEntityError(Exception):
    """Raise when the requested data entity type to extract is no supported by an extractor"""


class UnsupportedFileTypeError(Exception):
    """Raise when the requested data file type to extract is no supported by an extractor"""
