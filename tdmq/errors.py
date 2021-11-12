

class TdmqError(Exception):
    @property
    def description(self):
        return self.args[0] if self.args else None


class RequestException(TdmqError):
    pass


class QueryTooLargeException(RequestException):
    pass


class DuplicateItemException(RequestException):
    pass


class ItemNotFoundException(RequestException):
    pass


class UnsupportedFunctionality(TdmqError):
    pass


class DBOperationalError(TdmqError):
    pass
