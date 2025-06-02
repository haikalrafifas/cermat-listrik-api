import os

# Queue buffer size
BUFFER_SIZE = 20

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
