from typing import Optional
import logging

from supabase import supabase_client

# create a custom supabase handler
class SupabaseHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        # set some caching options - can't find a better way right now
        self.last_dataset_id: Optional[str] = None

    def emit(self, record: logging.LogRecord) -> None:
        # set the base information
        log = dict(
            name=record.name,
            level=record.levelname,
            message=self.format(record),
            origin=record.filename,
            origin_line=record.lineno,
        )

        # check if we have a metadata object
        if self.last_dataset_id is not None:
            log.update(
                file_id=self.last_dataset_id,
            )
            self.last_metadata = None

        # connect to the database and log
        with supabase_client() as client:
            # get the user
            log.update(user_id=client.user_id)
            client.table("logs").insert(log).execute()

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
console_formatter = logging.Formatter('[%(levelname)s]: %(message)s')

# set the formatter
console_handler.setFormatter(console_formatter)

# add the console handler to the logger
logger.addHandler(console_handler)
logger.addHandler(supabase_handler)