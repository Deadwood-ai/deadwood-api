import logging

from .supabase import use_client
from .__version__ import __version__
from .settings import settings


# create a custom supabase handler
class SupabaseHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # set the base information
        log = dict(
            name=record.name,
            level=record.levelname,
            message=self.format(record),
            origin=record.filename,
            origin_line=record.lineno,
            backend_version=__version__,
        )

        # check if we have a metadata object
        log.update(dataset_id=record.__dict__.get("dataset_id"))

        # add the user id
        log.update(user_id=record.__dict__.get("user_id"))

        # connect to the database and log
        with use_client(record.__dict__.get("token")) as client:
            try:
                client.table(settings.logs_table).insert(log).execute()
            except Exception as e:
                print(
                    f"An error occurred while trying to log to the database: {str(e)}"
                )


# create the logger
logger = logging.getLogger("processor")

# set the log level to debug
logger.setLevel(logging.DEBUG)

# create a stream handler for all messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

supabase_handler = SupabaseHandler()
supabase_handler.setLevel(logging.INFO)


# create a formatter for the console handler
console_formatter = logging.Formatter("[%(levelname)s]: %(message)s")

# set the formatter
console_handler.setFormatter(console_formatter)

# add the console handler to the logger
logger.addHandler(console_handler)
logger.addHandler(supabase_handler)
