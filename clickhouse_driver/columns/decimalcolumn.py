from decimal import Decimal, localcontext

from ..writer import MAX_UINT64, MAX_INT64
from ..util import compat
from .base import FormatColumn
from .exceptions import ColumnTypeMismatchException


class DecimalColumn(FormatColumn):
    py_types = (Decimal, float) + compat.integer_types
    max_precision = None
    int_size = None
    null_value = 0

    def __init__(self, precision, scale, types_check=False, **kwargs):
        self.precision = precision
        self.scale = scale
        super(DecimalColumn, self).__init__(**kwargs)

        if types_check:
            max_signed = (1 << (8 * self.int_size - 1)) - 1

            def check_items(items):
                for x in items:
                    if x is not None and (x < -max_signed or x > max_signed):
                        raise ColumnTypeMismatchException(x)

            self.check_items = check_items

    def after_read_items(self, items, nulls_map=None):
        if self.scale > 1:
            s = 10 ** self.scale

            # todo: inplace replace
            if nulls_map is not None:
                items = tuple([
                  (None if is_null else Decimal(items[i]) / s)
                  for i, is_null in enumerate(nulls_map)
                  ])

            else:
                items = tuple([Decimal(x) / s for x in items])

            return items
        else:
            if nulls_map is not None:
                items = tuple([
                  (None if is_null else Decimal(items[i]))
                  for i, is_null in enumerate(nulls_map)
                  ])

            else:
                items = tuple([Decimal(x) for x in items])

            return items

    def before_write_items(self, items):
        null_value = self.null_value

        if self.scale > 1:
            s = 10 ** self.scale

            if self.nullable:
                for i, x in enumerate(items):
                    if x is not None:
                        items[i] = int(Decimal(x) * s)
                    else:
                        items[i] = null_value

            else:
                for i, x in enumerate(items):
                    items[i] = int(Decimal(x) * s)

        else:
            if self.nullable:
                for i, x in enumerate(items):
                    if x is not None:
                        items[i] = int(Decimal(x))
                    else:
                        items[i] = null_value

            else:
                for i, x in enumerate(items):
                    items[i] = int(Decimal(x))

    # Override default precision to the maximum supported by underlying type.
    def _write_data(self, items, buf):
        with localcontext() as ctx:
            ctx.prec = self.max_precision
            super(DecimalColumn, self)._write_data(items, buf)

    def _read_data(self, n_items, buf, nulls_map=None):
        with localcontext() as ctx:
            ctx.prec = self.max_precision
            return super(DecimalColumn, self)._read_data(
                n_items, buf, nulls_map=nulls_map
            )


class Decimal32Column(DecimalColumn):
    format = 'i'
    max_precision = 9
    int_size = 4


class Decimal64Column(DecimalColumn):
    format = 'q'
    max_precision = 18
    int_size = 8


class Decimal128Column(DecimalColumn):
    format = 'Q'  # We manually deal with sign in read/write.
    max_precision = 38
    int_size = 16

    def write_items(self, items, buf):
        n_items = len(items)

        uint_64_pairs = [None] * 2 * n_items
        for i, x in enumerate(items):
            i2 = 2 * i

            # Differs from write_binary_uint128.
            # Lower 64 bits are written first.
            if x >= 0:
                uint_64_pairs[i2] = x & MAX_UINT64
                uint_64_pairs[i2 + 1] = (x >> 64) & MAX_UINT64
            else:
                x = -x
                uint_64_pairs[i2] = MAX_UINT64 - (x & MAX_UINT64) + 1
                uint_64_pairs[i2 + 1] = MAX_UINT64 - ((x >> 64) & MAX_UINT64)

        s = self.make_struct(2 * n_items)
        buf.write(s.pack(*uint_64_pairs))

    def read_items(self, n_items, buf):
        s = self.make_struct(2 * n_items)
        items = s.unpack(buf.read(s.size))

        int_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            # Differs from read_binary_uint128.
            # Lower 64 bits are read first.
            if items[i2 + 1] > MAX_INT64:
                int_128_items[i] = (
                    -((MAX_UINT64 - items[i2 + 1]) << 64) -
                    (MAX_UINT64 - items[i2]) - 1
                )

            else:
                int_128_items[i] = (items[i2 + 1] << 64) + items[i2]

        return int_128_items


def create_decimal_column(spec, column_options):
    precision, scale = spec[8:-1].split(',')
    precision, scale = int(precision), int(scale)

    # Maximum precisions for underlying types are:
    # Int32    9
    # Int64   18
    # Int128  38
    if precision <= 9:
        cls = Decimal32Column
    elif precision <= 18:
        cls = Decimal64Column
    else:
        cls = Decimal128Column

    return cls(precision, scale, **column_options)
