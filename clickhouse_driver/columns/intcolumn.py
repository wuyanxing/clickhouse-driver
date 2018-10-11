
from ..util import compat
from .exceptions import ColumnTypeMismatchException
from .base import FormatColumn


class IntColumn(FormatColumn):
    py_types = compat.integer_types
    int_size = None
    null_value = 0

    def __init__(self, types_check=False, **kwargs):
        super(IntColumn, self).__init__(types_check=types_check, **kwargs)

    def before_write_items(self, items):
        if self.types_check_enabled:
            mask = (1 << 8 * self.int_size) - 1
            null_value = self.null_value

            # Chop only bytes that fit current type.
            # ctypes.c_intXX is slower.

            if self.nullable:
                for i, x in enumerate(items):
                    if x is not None:
                        if x >= 0:
                            sign = 1
                        else:
                            sign = -1
                            x = -x

                        items[i] = sign * (x & mask)
                    else:
                        items[i] = null_value

            else:
                for i, x in enumerate(items):
                    if x >= 0:
                        sign = 1
                    else:
                        sign = -1
                        x = -x

                    items[i] = sign * (x & mask)
        else:
            null_value = self.null_value

            if self.nullable:
                for i, x in enumerate(items):
                    if x is None:
                        items[i] = null_value


class UIntColumn(IntColumn):
    def __init__(self, types_check=False, **kwargs):
        super(UIntColumn, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            def check_items(items):
                for x in items:
                    if x is not None and x < 0:
                        raise ColumnTypeMismatchException(x)

            self.check_items = check_items


class Int8Column(IntColumn):
    ch_type = 'Int8'
    format = 'b'
    int_size = 1


class Int16Column(IntColumn):
    ch_type = 'Int16'
    format = 'h'
    int_size = 2


class Int32Column(IntColumn):
    ch_type = 'Int32'
    format = 'i'
    int_size = 4


class Int64Column(IntColumn):
    ch_type = 'Int64'
    format = 'q'
    int_size = 8


class UInt8Column(UIntColumn):
    ch_type = 'UInt8'
    format = 'B'
    int_size = 1


class UInt16Column(UIntColumn):
    ch_type = 'UInt16'
    format = 'H'
    int_size = 2


class UInt32Column(UIntColumn):
    ch_type = 'UInt32'
    format = 'I'
    int_size = 4


class UInt64Column(UIntColumn):
    ch_type = 'UInt64'
    format = 'Q'
    int_size = 8
