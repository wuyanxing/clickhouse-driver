
from .. import errors
from ..writer import write_varint
from ..util import compat
from .base import Column

from codecs import utf_8_decode, utf_8_encode


class String(Column):
    ch_type = 'String'
    py_types = compat.string_types
    null_value = ''

    # TODO: pass user encoding here

    def write_items(self, items, buf):
        if self.nullable:
            null_value = self.null_value

            for x in items:
                if x is None:
                    x = null_value

                if not isinstance(x, bytes):
                    x = utf_8_encode(x)[0]

                write_varint(len(x), buf)
                buf.write(x)

        else:
            for x in items:
                if not isinstance(x, bytes):
                    x = utf_8_encode(x)[0]

                write_varint(len(x), buf)
                buf.write(x)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items, decode=True)


class ByteString(String):
    py_types = (bytearray, bytes)

    def write_items(self, items, buf):
        null_value = self.null_value

        if self.nullable:
            for x in items:
                if x is None:
                    x = null_value

                write_varint(len(x), buf)
                buf.write(x)
        else:
            for x in items:
                write_varint(len(x), buf)
                buf.write(x)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items)


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        length = self.length
        items = [None] * n_items
        items_buf_view = memoryview(buf.read(length * n_items))
        buf_pos = 0

        for i in compat.range(n_items):
            value = items_buf_view[buf_pos:buf_pos + length].tobytes() \
                .rstrip(b'\x00')

            try:
                value = utf_8_decode(value)[0]
            except UnicodeDecodeError:
                pass

            items[i] = value
            buf_pos += length

        return items

    def write_items(self, items, buf):
        length = self.length
        items_buf = bytearray(length * len(items))
        items_buf_view = memoryview(items_buf)
        buf_pos = 0

        null_value = self.null_value

        if self.nullable:
            for x in items:
                if x is None:
                    x = null_value

                if not isinstance(x, bytes):
                    x = utf_8_encode(x)[0]

                value_len = len(x)
                if length < value_len:
                    raise errors.TooLargeStringSize()

                items_buf_view[buf_pos:buf_pos + min(length, value_len)] = x
                buf_pos += length
        else:
            for x in items:
                if not isinstance(x, bytes):
                    x = utf_8_encode(x)[0]

                value_len = len(x)
                if length < value_len:
                    raise errors.TooLargeStringSize()

                items_buf_view[buf_pos:buf_pos + min(length, value_len)] = x
                buf_pos += length

        buf.write(items_buf)


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)

    def read_items(self, n_items, buf):
        length = self.length
        items = [None] * n_items
        items_buf_view = memoryview(buf.read(length * n_items))
        buf_pos = 0

        for i in compat.range(n_items):
            items[i] = items_buf_view[buf_pos:buf_pos + length].tobytes()
            buf_pos += length

        return items

    def write_items(self, items, buf):
        length = self.length
        items_buf = bytearray(length * len(items))
        items_buf_view = memoryview(items_buf)
        buf_pos = 0

        if self.nullable:
            null_value = self.null_value

            for x in items:
                if x is None:
                    x = null_value
                    value_len = 0
                else:
                    value_len = len(x)
                    if length < value_len:
                        raise errors.TooLargeStringSize()

                items_buf_view[buf_pos:buf_pos + min(length, value_len)] = x
                buf_pos += length
        else:
            for x in items:
                value_len = len(x)
                if length < value_len:
                    raise errors.TooLargeStringSize()

                items_buf_view[buf_pos:buf_pos + min(length, value_len)] = x
                buf_pos += length

        buf.write(items_buf)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']

    if spec == 'String':
        cls = ByteString if strings_as_bytes else String
        return cls(**column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length, **column_options)
