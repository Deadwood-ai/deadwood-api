import logging

from .supabase import use_client

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
        )

        # check if we have a metadata object
        if hasattr(self, 'dataset_id'):
            log.update(
                file_id=self.dataset_id,
            )
        
        # set that there is no token
        if not hasattr(self, 'token'):
            self.token = None
        
        # add the user id
        if hasattr(self, 'user_id'):
            log.update(
                user_id=self.user_id
            )

        # connect to the database and log
        with use_client(self.token) as client:
            # set an empty user id if there is none
            if not hasattr(client, 'user_id'):
                self.user_id = getattr(client, 'user_id', None)

            log.update(user_id=self.user_id)
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