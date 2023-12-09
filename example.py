import shlex
from client import RedisClient

k, v = 'example_key', 'hello_world'

client = RedisClient()
r = client.set(k, v)
print('set', r)

r = client.get(k)
print('get', r)

r = client.exists(k)
print('exists', r)

r = client.delete(k)
print('delete', r)

r = client.get(k)
print('get', r)

r = client.exists(k)
print('exists', r)
