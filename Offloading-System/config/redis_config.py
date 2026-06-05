import os

# Redis 与 server 同机即可（device_client 不直接使用 Redis，只有 server/user 用）。
REDIS_HOST = os.environ.get('OPENSAI_REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('OPENSAI_REDIS_PORT', '6379'))
REDIS_DB = int(os.environ.get('OPENSAI_REDIS_DB', '0'))

