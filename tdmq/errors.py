

class TdmqError(Exception):
    pass


class DuplicateItemException(TdmqError):
    pass


class ItemNotFoundException(TdmqError):
    pass
