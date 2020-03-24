

class TdmqError(Exception):
    pass


class RequestException(TdmqError):
    pass


class DuplicateItemException(RequestException):
    pass


class ItemNotFoundException(RequestException):
    pass


class UnsupportedFunctionality(TdmqError):
    pass


class DBOperationalError(TdmqError):
    pass