from enum import Enum

from .. import errors
from ..util import compat
from .intcolumn import IntColumn


class EnumColumn(IntColumn):
    py_types = (Enum, ) + compat.integer_types + compat.string_types
    null_value = 0

    def __init__(self, enum_cls, **kwargs):
        self.enum_cls = enum_cls
        super(EnumColumn, self).__init__(**kwargs)

    def before_write_items(self, items):
        enum_cls = self.enum_cls
        string_types = compat.string_types

        # Check real enum value
        try:
            if self.nullable:
                null_value = self.null_value

                for i, x in enumerate(items):
                    if x is not None:

                        if isinstance(x, Enum):
                            x = x.name

                        if isinstance(x, string_types):
                            items[i] = enum_cls[x].value
                        else:
                            items[i] = enum_cls(x).value
                    else:
                        items[i] = null_value
            else:
                for i, x in enumerate(items):
                    if isinstance(x, Enum):
                        x = x.name

                    if isinstance(x, string_types):
                        items[i] = enum_cls[x].value
                    else:
                        items[i] = enum_cls(x).value

        except (ValueError, KeyError):
            choices = ', '.join(
                "'{}' = {}".format(x.name, x.value) for x in enum_cls
            )
            enum_str = '{}({})'.format(enum_cls.__name__, choices)

            raise errors.LogicalError(
                "Unknown element '{}' for type {}".format(x, enum_str)
            )

    def after_read_items(self, items, nulls_map=None):
        enum_cls = self.enum_cls

        if nulls_map is not None:
            items = tuple([
                (None if is_null else enum_cls(items[i]).name)
                for i, is_null in enumerate(nulls_map)
            ])

        else:
            items = tuple([enum_cls(x).name for x in items])

        return items


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = 'b'
    int_size = 1


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = 'h'
    int_size = 2


def create_enum_column(spec, column_options):
    if spec.startswith('Enum8'):
        params = spec[6:-1]
        cls = Enum8Column
    else:
        params = spec[7:-1]
        cls = Enum16Column

    d = {}
    for param in params.split(", '"):
        pos = param.rfind("'")
        name = param[:pos].lstrip("'")
        value = int(param[pos + 1:].lstrip(' ='))
        d[name] = value

    return cls(Enum(cls.ch_type, d), **column_options)
