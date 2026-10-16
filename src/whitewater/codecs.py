"""Bidirectional object ↔ wire-bytes serialization — the wire always
carries raw bytes; a :class:`Codec` keeps that plumbing inside the SDK
so application code works with its own types::

    orders = client.feed("orders.events", value_codec=codecs.json_of(Order))
    orders.writer("ingest").append("order-1", Order("order-1", 42))

    row = client.state_store("app.users", value_codec=codecs.json_of(User)).get("u1")
    user = row.payload                     # decoded — never .encode()/.decode()

``utf8()`` for text keys/payloads, ``raw()`` for binary, ``json_()`` for
untyped JSON, ``json_of(type)`` for dataclasses/classes, and ``of()``
for anything else (protobuf, msgpack, application framing).
"""

from __future__ import annotations

import dataclasses
import json as _json
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class Codec(Generic[T]):
    """Object ↔ wire-bytes pair. Construct via the module factories
    (``utf8()``, ``json_of(Order)``, ``of(enc, dec)``) or subclass."""

    def encode(self, value: T) -> bytes:  # pragma: no cover - interface
        raise NotImplementedError

    def decode(self, data: bytes) -> T:  # pragma: no cover - interface
        raise NotImplementedError


class _BytesCodec(Codec[bytes]):
    """Identity codec — raw bytes through to the wire. Encoding is
    deliberately tolerant: ``str`` values take UTF-8 so a raw-bound
    facade still accepts the text shorthand."""

    def encode(self, value) -> bytes:
        if value is None:
            return None
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)

    def decode(self, data: bytes) -> bytes:
        return data


class _Utf8Codec(Codec[str]):
    """UTF-8 text — the common case for keys and JSON/text payloads.
    Encoding tolerates ``bytes`` (passed through) so mixed call sites
    keep working under the default binding."""

    def encode(self, value) -> bytes:
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        return str(value).encode("utf-8")

    def decode(self, data: bytes) -> str:
        return data.decode("utf-8")


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if hasattr(value, "__dict__"):
        return vars(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"value is not JSON-serializable: {type(value).__name__}")


def _construct(type_: Type[T], obj: Any) -> T:
    if obj is None:
        return None
    if dataclasses.is_dataclass(type_) and isinstance(obj, dict):
        return type_(**obj)
    if isinstance(obj, type_):
        return obj
    if isinstance(obj, dict):
        try:
            return type_(**obj)
        except TypeError:
            pass
    return type_(obj)


class _JsonCodec(Codec[Any]):
    """JSON via :mod:`json` — dataclasses (asdict round-trip), plain
    objects (``vars()`` / ``type_(**fields)``), or untyped trees when no
    ``type_`` is given."""

    def __init__(self, type_: Optional[Type] = None):
        self._type = type_

    def encode(self, value) -> bytes:
        return _json.dumps(
            value, default=_json_default, separators=(",", ":")
        ).encode("utf-8")

    def decode(self, data: bytes):
        obj = _json.loads(data.decode("utf-8"))
        return _construct(self._type, obj) if self._type is not None else obj


class _FuncCodec(Codec[T]):
    def __init__(
        self,
        encoder: Callable[[T], bytes],
        decoder: Callable[[bytes], T],
    ):
        self._encoder = encoder
        self._decoder = decoder

    def encode(self, value: T) -> bytes:
        return self._encoder(value)

    def decode(self, data: bytes) -> T:
        return self._decoder(data)


_BYTES = _BytesCodec()
_UTF8 = _Utf8Codec()
_JSON = _JsonCodec()


def raw() -> Codec[bytes]:
    """Identity codec — raw bytes through to the wire."""
    return _BYTES


def utf8() -> Codec[str]:
    """UTF-8 text — the common case for keys and JSON/text payloads."""
    return _UTF8


def json_() -> Codec[Any]:
    """Untyped JSON payloads — dicts/lists/scalars via ``json.loads``."""
    return _JSON


def json_of(type_: Type[T]) -> Codec[T]:
    """JSON-mapped objects — dataclasses and plain classes round-trip
    field-for-field; ``type_(**fields)`` constructs on decode."""
    return _JsonCodec(type_)


def of(
    encoder: Callable[[T], bytes],
    decoder: Callable[[bytes], T],
) -> Codec[T]:
    """A codec from caller-supplied functions — protobuf, msgpack,
    application framing::

        proto = codecs.of(User.SerializeToString, User.FromString)
    """
    return _FuncCodec(encoder, decoder)


def for_type(type_: Type[T]) -> Codec[T]:
    """Codec by type: ``str`` is UTF-8, ``bytes`` is raw identity,
    ``dict``/``list``/``None`` are untyped JSON, and every other type is
    JSON-mapped — so ``client.feed(name, value_type=Order)`` just works
    without spelling ``json_of(...)``."""
    if type_ is None or type_ in (dict, list):
        return _JSON  # type: ignore[return-value]
    if type_ is str:
        return _UTF8  # type: ignore[return-value]
    if type_ is bytes:
        return _BYTES  # type: ignore[return-value]
    return _JsonCodec(type_)


__all__ = ["Codec", "raw", "utf8", "json_", "json_of", "of", "for_type"]
