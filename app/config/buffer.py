import os

# Queue buffer size
MAX_BUFFER_SIZE = 3600 # data

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
