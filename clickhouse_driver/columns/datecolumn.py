from datetime import date, timedelta

from .base import FormatColumn


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    epoch_start = date(1970, 1, 1)

    def before_write_item(self, value):
        return (value - self.epoch_start).days

    def after_read_items(self, items, nulls_map=None):
        epoch_start = self.epoch_start

        if nulls_map is not None:
            items = tuple([
                (None if is_null else epoch_start + timedelta(items[i]))
                for i, is_null in enumerate(nulls_map)
            ])

        else:
            items = tuple([epoch_start + timedelta(x) for x in items])

        return items
