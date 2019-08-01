

class TdmqError(Exception):
    pass


class RequestException(TdmqError):
    pass


class DuplicateItemException(RequestException):
    pass


class ItemNotFoundException(RequestException):
    pass
