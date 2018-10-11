from ctypes import c_float

from .base import FormatColumn


class FloatColumn(FormatColumn):
    py_types = (float, int)


class Float32(FloatColumn):
    ch_type = 'Float32'
    format = 'f'
    null_value = 0

    def __init__(self, types_check=False, **kwargs):
        super(Float32, self).__init__(types_check=types_check, **kwargs)

    def before_write_items(self, items):
        null_value = self.null_value

        if self.types_check_enabled:
            # Chop only bytes that fit current type.
            # Cast to -nan or nan if overflows.
            if self.nullable:
                for i, x in enumerate(items):
                    if x is not None:
                        items[i] = c_float(x).value
                    else:
                        items[i] = null_value

            else:
                for i, x in enumerate(items):
                    items[i] = c_float(x).value
        else:
            if self.nullable:
                for i, x in enumerate(items):
                    if x is None:
                        items[i] = null_value


class Float64(FloatColumn):
    ch_type = 'Float64'
    format = 'd'
