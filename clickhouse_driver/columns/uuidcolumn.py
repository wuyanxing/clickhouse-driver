from uuid import UUID

from .base import FormatColumn
from .. import errors
from ..util import compat
from ..writer import MAX_UINT64


class UUIDColumn(FormatColumn):
    ch_type = 'UUID'
    py_types = compat.string_types + (UUID, )
    format = 'Q'
    null_value = 0

    # UUID is stored by two uint64 numbers.
    def write_items(self, items, buf):
        n_items = len(items)

        uint_64_pairs = [None] * 2 * n_items
        for i, x in enumerate(items):
            i2 = 2 * i
            uint_64_pairs[i2] = (x >> 64) & MAX_UINT64
            uint_64_pairs[i2 + 1] = x & MAX_UINT64

        s = self.make_struct(2 * n_items)
        buf.write(s.pack(*uint_64_pairs))

    def read_items(self, n_items, buf):
        s = self.make_struct(2 * n_items)
        items = s.unpack(buf.read(s.size))

        uint_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            uint_128_items[i] = (items[i2] << 64) + items[i2 + 1]

        return uint_128_items

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is not None:
            items = tuple([
                (None if is_null else UUID(int=items[i]))
                for i, is_null in enumerate(nulls_map)
            ])

        else:
            items = tuple([UUID(int=x) for x in items])

        return items

    def before_write_items(self, items):
        try:
            if self.nullable:
                null_value = self.null_value

                for i, x in enumerate(items):
                    if x is not None:
                        if not isinstance(x, UUID):
                            x = UUID(x)

                        items[i] = x.int
                    else:
                        items[i] = null_value

            else:
                for i, x in enumerate(items):
                    if not isinstance(x, UUID):
                        x = UUID(x)

                    items[i] = x.int

        except ValueError:
            raise errors.CannotParseUuidError(
                "Cannot parse uuid '{}'".format(x)
            )
