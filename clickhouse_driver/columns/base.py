from struct import Struct, error as struct_error

from . import exceptions


class Column(object):
    ch_type = None
    py_types = None

    check_items = None
    after_read_items = None
    before_write_items = None

    types_check_enabled = False

    def __init__(self, types_check=False, **kwargs):
        self.nullable = False
        self.types_check_enabled = types_check
        super(Column, self).__init__()

    def make_null_struct(self, n_items):
        return Struct('<{}B'.format(n_items))

    def _read_nulls_map(self, n_items, buf):
        s = self.make_null_struct(n_items)
        return s.unpack(buf.read(s.size))

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        items = [x is None for x in items]
        buf.write(s.pack(*items))

    def prepare_items(self, items):
        if self.types_check_enabled:
            for x in items:
                if x is not None and not isinstance(x, self.py_types):
                    raise exceptions.ColumnTypeMismatchException(x)

        if self.check_items:
            self.check_items(items)

        if self.before_write_items:
            self.before_write_items(items)

    def write_data(self, items, buf):
        """
        :param items: list of items
        :param buf:
        :return:
        """
        if self.nullable:
            self._write_nulls_map(items, buf)

        self._write_data(items, buf)

    def _write_data(self, items, buf):
        self.prepare_items(items)
        self.write_items(items, buf)

    def write_items(self, items, buf):
        raise NotImplementedError

    def read_data(self, n_items, buf):
        if self.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)
        else:
            nulls_map = None

        return self._read_data(n_items, buf, nulls_map=nulls_map)

    def _read_data(self, n_items, buf, nulls_map=None):
        items = self.read_items(n_items, buf)

        if self.after_read_items:
            items = self.after_read_items(items, nulls_map=nulls_map)
        elif nulls_map:
            items = tuple([
                (None if is_null else items[i])
                for i, is_null in enumerate(nulls_map)
            ])

        return items

    def read_items(self, n_items, buf):
        raise NotImplementedError


class FormatColumn(Column):
    """
    Uses struct.pack for bulk items writing.
    """

    format = None

    def make_struct(self, n_items):
        return Struct('<{}{}'.format(n_items, self.format))

    def write_items(self, items, buf):
        s = self.make_struct(len(items))
        try:
            buf.write(s.pack(*items))

        except struct_error as e:
            raise exceptions.StructPackException(e)

    def read_items(self, n_items, buf):
        s = self.make_struct(n_items)
        return s.unpack(buf.read(s.size))
