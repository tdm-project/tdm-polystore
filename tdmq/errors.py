

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
        return s

    __str__ = __repr__


class TdmqBadRequestException(TdmqError):
    def __init__(self, msg: str = None, status: int = 400):
        super().__init__("Bad Request", status, msg)


class DuplicateItemException(TdmqError):
    def __init__(self, msg: str = None, status: int = 409):
        super().__init__("Duplicate entity", status, msg)


class ItemNotFoundException(TdmqError):
    def __init__(self, msg: str = None, status: int = 404):
        super().__init__("Item not found", status, msg)


class QueryTooLargeException(TdmqError):
    def __init__(self, msg: str = None, status: int = 413):
        super().__init__("Query Result Too Large", status, msg)


class UnauthorizedError(TdmqError):
    def __init__(self, msg: str = None, status: int = 401):
        super().__init__("Unauthorized", status, msg)


class ForbiddenError(TdmqError):
    def __init__(self, msg: str = None, status: int = 403):
        super().__init__("Forbidden", status, msg)


class InternalServerError(TdmqError):
    def __init__(self, msg: str = None, status: int = 500):
        super().__init__("Internal server error", status, msg)


class UnsupportedFunctionality(TdmqError):
    def __init__(self, msg: str = None, status: int = 501):
        super().__init__("Unsupported functionality", status, msg)


class DBOperationalError(InternalServerError):
    pass
