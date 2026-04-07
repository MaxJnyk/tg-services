from uuid import UUID, uuid4
from typing import Optional


class PostId:
    def __init__(self, value: Optional[UUID] = None):
        self._value = value or uuid4()

    @property
    def value(self) -> UUID:
        return self._value

    def __str__(self) -> str:
        return str(self._value)

    def __eq__(self, other) -> bool:
        return str(self) == str(other)
