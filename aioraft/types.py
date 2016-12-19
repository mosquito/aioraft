#!/usr/bin/env python
# encoding: utf-8
import struct
from enum import unique, Enum

__all__ = 'Command', 'Code', 'Types'


@unique
class Types(str, Enum):
    uchar = '!B', 1
    char = '!b', 1

    bool = '?'

    ulong = '!Q', 8
    long = '!q', 8

    ushort = '!H', 2
    short = '!h', 2

    int = '!i', 4
    uint = '!I', 4

    float = 'f', 4
    double = 'd', 8

    def __new__(cls, value, length):
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj._length = length
        return obj

    def __len__(self):
        return self._length

    def pack(self, item):
        return struct.pack(self.value, item)

    def unpack(self, value):
        return struct.unpack(self.value, value)[0]


def unpack_enum(enum, value, _type=Types.char):
    return enum(_type.unpack(value))


class StructEnum(int):
    _TYPE = None

    def pack(self):
        return self._TYPE.pack(self.value)

    @classmethod
    def unpack(cls, item):
        return unpack_enum(cls, item, _type=cls._TYPE)

    def __len__(self):
        return len(self._TYPE)


class UCharEnum(StructEnum):
    _TYPE = Types.uchar


class UShortEnum(StructEnum):
    _TYPE = Types.ushort


@unique
class Command(UCharEnum, Enum):
    # 255 types of commands
    MAP = 0x0
    SET = 0x1
    GET = 0x2
    JOIN = 0x3
    REPLICATE = 0x4


@unique
class Code(UShortEnum, Enum):
    # 65535 error codes
    OK = 200
    INTERNAL_SERVER_ERROR = 500
