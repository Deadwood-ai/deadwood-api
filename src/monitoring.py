from prometheus_client import Counter, Histogram


# create metrics for the upload route
# create a number of prometheus metrics
uploads_invoked = Counter('uploads_invoked', 'Number of uploads invoked')
uploads_counter = Counter('uploads_counter', 'Number of uploads, finishing without error.')
upload_time = Histogram('upload_seconds', 'Time spent in dataset uploads')
upload_size = Histogram('upload_bytes', 'Size of dataset uploads in bytes')

# create metrics for the cog route
cog_invoked = Counter('cog_invoked', 'Number of cogs invoked')
cog_counter = Counter('cog_counter', 'Number of cogs, finishing without error.')
cog_time = Histogram('cog_seconds', 'Time spent in cog creation')
cog_size = Histogram('cog_bytes', 'Size of cog files in bytes')

# create metrics for the metadata routes
metadata_invoked = Counter('metadata_invoked', 'Number of metadata upserting invoked')
metadata_counter = Counter('metadata_counter', 'Number of metadata upserting, finishing without error.')

# create metrics for the label routes
label_invoked = Counter('label_invoked', 'Number of label upserting invoked')
label_counter = Counter('label_counter', 'Number of label upserting, finishing without error.')