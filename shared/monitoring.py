import os
import logfire

# get the dev mode from the environment
dev_mode = os.environ.get('DEV_MODE', 'false').lower() == 'true'
# first thing we to is configure logfire
logfire.configure(environment='development' if dev_mode else 'production')

# uncomment here to disable system-metrics logging
# I am not enirely sure, what exactly this measures inside a docker container
logfire.instrument_system_metrics({
    'process.runtime.memory': ['rss', 'vms'],
    'process.runtime.cpu.utilization': None,
    'system.disk.io': ['read', 'write'],
    'system.disk.time': ['read', 'write'],
    'system.memory.usage': ['available', 'used'] 
}, base=None)

# instrument pydantic model validation
logfire.instrument_pydantic()