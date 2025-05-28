from typing import Any, Literal, overload
from .doubly_linked_list import DoublyLinkedList


class Datastore:
    _store: dict[bytes, Any] = {}

    def delete(self, key: bytes) -> int:
        return int(self._store.pop(key, None) is not None)

    # String #

    def strset(self, key: bytes, value: bytes) -> bool:
        self._store[key] = value
        return True

    def strget(self, key: bytes) -> bytes | None:
        value = self._store.get(key)
        return value if isinstance(value, bytes) else None

    # List #

    def lpush(self, key: bytes, *values: bytes) -> int:
        if key not in self._store or not isinstance(
            self._store[key], DoublyLinkedList
        ):
            self._store[key] = DoublyLinkedList()
        return self._store[key].lpush(*values)

    def rpush(self, key: bytes, *values: bytes) -> int:
        if key not in self._store or not isinstance(
            self._store[key], DoublyLinkedList
        ):
            self._store[key] = DoublyLinkedList()
        return self._store[key].rpush(*values)

    def lrange(self, key: bytes, start: int, end: int) -> list[bytes]:
        if key not in self._store or not isinstance(
            self._store[key], DoublyLinkedList
        ):
            return []
        return self._store[key].lrange(start, end)

    # Set #

    def sadd(self, key: bytes, *members: bytes) -> int:
        if key not in self._store or not isinstance(self._store[key], set):
            self._store[key] = set()
        before = len(self._store[key])
        self._store[key].update(members)
        return len(self._store[key]) - before

    def smembers(self, key: bytes) -> set[bytes]:
        return (
            self._store.get(key, set())
            if isinstance(self._store.get(key), set)
            else set()
        )

    # Hash #

    @overload
    def hset(
        self, key: bytes, field: bytes, value: bytes, *, mapping: None = ...
    ) -> int: ...

    @overload
    def hset(
        self,
        key: bytes,
        field: None = ...,
        value: None = ...,
        *,
        mapping: dict[bytes, bytes],
    ) -> int: ...

    def hset(
        self,
        key: bytes,
        field: bytes | None = None,
        value: bytes | None = None,
        *,
        mapping: dict[bytes, bytes] | None = None,
    ) -> int:
        if mapping is not None:
            if field is not None or value is not None:
                raise TypeError(
                    'Specify either field+value or mapping, not both'
                )
            if key not in self._store or not isinstance(self._store[key], dict):
                self._store[key] = {}
            new_fields = 0
            for f, v in mapping.items():
                if f not in self._store[key]:
                    new_fields += 1
                self._store[key][f] = v
            return new_fields
        elif field is not None and value is not None:
            if key not in self._store or not isinstance(self._store[key], dict):
                self._store[key] = {}
            is_new = field not in self._store[key]
            self._store[key][field] = value
            return int(is_new)
        else:
            raise TypeError('You must specify either field+value or mapping')

    def hget(self, key: bytes, field: bytes) -> bytes | None:
        return (
            self._store.get(key, {}).get(field)
            if isinstance(self._store.get(key), dict)
            else None
        )

    def hgetall(self, key: bytes) -> dict[bytes, bytes]:
        return (
            self._store.get(key, {})
            if isinstance(self._store.get(key), dict)
            else {}
        )

    # Sorted set #

    def zadd(self, key: bytes, mapping: dict[bytes, float]) -> int:
        if key not in self._store or not isinstance(self._store[key], dict):
            self._store[key] = {}
        added = 0
        for member, score in mapping.items():
            if member not in self._store[key]:
                added += 1
            self._store[key][member] = score
        return added

    @overload
    def zrange(
        self, key: bytes, start: int, end: int, withscores: Literal[True]
    ) -> list[tuple[bytes, int]]: ...

    @overload
    def zrange(
        self, key: bytes, start: int, end: int, withscores: Literal[False]
    ) -> list[bytes]: ...

    @overload
    def zrange(self, key: bytes, start: int, end: int) -> list[bytes]: ...


    def zrange(
        self, key: bytes, start: int, end: int, withscores: bool = False
    ) -> list[tuple[bytes, int]] | list[bytes]:
        if key not in self._store or not isinstance(self._store[key], dict):
            return []
        sorted_items = sorted(
            self._store[key].items(), key=lambda item: item[1]
        )
        if end == -1:
            end = len(sorted_items)
        else:
            end += 1
        return (
            sorted_items[start:end]
            if withscores
            else [k for k, _ in sorted_items[start:end]]
        )
