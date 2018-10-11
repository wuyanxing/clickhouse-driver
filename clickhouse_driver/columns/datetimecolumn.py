from calendar import timegm
from datetime import datetime
from time import mktime

from pytz import timezone as get_timezone, utc

from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    py_types = (datetime, int)
    format = 'I'
    null_value = 0

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

    def before_write_items(self, items):
        tz_localize = self.timezone.localize if self.timezone else None

        if self.nullable:
            null_value = self.null_value

            for i, x in enumerate(items):
                if x is not None:
                    if isinstance(x, int):
                        # support supplying raw integers to avoid
                        # costly timezone conversions when using datetime
                        continue

                    if tz_localize:
                        # Set server's timezone for offset-naive datetime.
                        if x.tzinfo is None:
                            x = tz_localize(x)

                        x = x.astimezone(utc)
                        items[i] = int(timegm(x.timetuple()))

                    else:
                        # If datetime is offset-aware use it's timezone.
                        if x.tzinfo is not None:
                            x = x.astimezone(utc)
                            items[i] = int(timegm(x.timetuple()))
                        else:
                            items[i] = int(mktime(x.timetuple()))
                else:
                    items[i] = null_value

        else:
            for i, x in enumerate(items):
                if isinstance(x, int):
                    # support supplying raw integers to avoid
                    # costly timezone conversions when using datetime
                    continue

                if tz_localize:
                    # Set server's timezone for offset-naive datetime.
                    if x.tzinfo is None:
                        x = tz_localize(x)

                    x = x.astimezone(utc)
                    items[i] = int(timegm(x.timetuple()))

                else:
                    # If datetime is offset-aware use it's timezone.
                    if x.tzinfo is not None:
                        x = x.astimezone(utc)
                        items[i] = int(timegm(x.timetuple()))
                    else:
                        items[i] = int(mktime(x.timetuple()))


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
