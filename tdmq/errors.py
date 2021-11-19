

class TdmqError(Exception):
    def __init__(self, title: str = "", status: int = 500, detail: str = None):
        super().__init__(title)
        self._title = title
        self._status = status
        self._detail = detail

    @property
    def status(self) -> int:
        return self._status or 500

    @property
    def title(self) -> str:
        return self._title

    @property
    def detail(self) -> str:
        return self._detail

    def __repr__(self) -> str:
        s = f"[{self._status}] {self._title}"
        if self._detail:
            s += ": " + self._detail

    __str__ = __repr__


class TdmqBadRequestException(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Bad Request", 400, msg)


class QueryTooLargeException(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Query Result Too Large", 413, msg)


class DuplicateItemException(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Duplicate entity", 400, msg)


class ItemNotFoundException(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Item not found", 404, msg)


class UnsupportedFunctionality(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Unsupported functionality", 501, msg)


class InternalServerError(TdmqError):
    def __init__(self, msg: str = None):
        super().__init__("Internal server error", 500, msg)


class DBOperationalError(InternalServerError):
    pass
