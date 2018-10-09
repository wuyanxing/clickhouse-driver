from calendar import timegm
from datetime import datetime
from time import mktime

from pytz import timezone as get_timezone, utc

from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    py_types = (datetime, int)
    format = 'I'

    def __init__(self, timezone=None, **kwargs):
        self.timezone = timezone
        super(DateTimeColumn, self).__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        fts = datetime.fromtimestamp
        tz = self.timezone

        if nulls_map is not None:
            items = tuple([
                (None if is_null else fts(items[i], tz).replace(tzinfo=None))
                for i, is_null in enumerate(nulls_map)
            ])

        else:
            items = tuple([fts(x, tz).replace(tzinfo=None) for x in items])

        return items

    def before_write_item(self, value):
        if isinstance(value, int):
            # support supplying raw integers to avoid
            # costly timezone conversions when using datetime
            return value

        if self.timezone:
            # Set server's timezone for offset-naive datetime.
            if value.tzinfo is None:
                value = self.timezone.localize(value)

            value = value.astimezone(utc)
            return int(timegm(value.timetuple()))

        else:
            # If datetime is offset-aware use it's timezone.
            if value.tzinfo is not None:
                value = value.astimezone(utc)
                return int(timegm(value.timetuple()))

            return int(mktime(value.timetuple()))


def create_datetime_column(spec, column_options):
    context = column_options['context']

    tz_name = timezone = None

    # Use column's timezone if it's specified.
    if spec[-1] == ')':
        tz_name = spec[10:-2]
    else:
        if not context.settings.get('use_client_time_zone', False):
            tz_name = context.server_info.timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return DateTimeColumn(timezone=timezone, **column_options)
