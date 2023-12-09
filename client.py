from io import TextIOBase
from typing import Optional
import re
import socket


class RedisException(Exception):
    pass


# TODO: # Add support for keys/values with spaces. Currently you must enclose a string with spaces in quotes.
class RedisClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        timeout_s: Optional[float] = 30.0,
    ):
        """Create socket connection to a Redis service"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.socket.settimeout(timeout_s)
        self.rr = _ResponseReader(self.socket.makefile("r"))

    def close(self):
        self.socket.close()

    def set(self, key: str, value: str) -> bool:
        """Set key to hold the string value. Return True if successful, otherwise raises a RedisException"""
        command = f"SET {key} {value}\r\n"
        self.socket.send(command.encode())
        return self.rr.read_ok()

    def get(self, key: str) -> Optional[str]:
        """Get the value of key. If the key does not exist NULL is returned."""
        command = f"GET {key}\r\n"
        self.socket.send(command.encode())
        return self.rr.read_string()

    def delete(self, key: str) -> int:
        """Removes the specified keys. Returns the number of keys that were removed."""
        command = f"DEL {key}\r\n"
        self.socket.send(command.encode())
        return self.rr.read_integer()

    def exists(self, key: str) -> int:
        """Check if key exists. Return specifically the number of keys that exist from those specified as arguments."""
        command = f"EXISTS {key}\r\n"
        self.socket.send(command.encode())
        return self.rr.read_integer()


class _ResponseReader:
    def __init__(self, fp: TextIOBase):
        self._fp = fp

    def read_integer(self) -> int:
        line = self._fp.readline().rstrip("\r\n")
        if line.startswith("-"):
            raise RedisException(line[1:])
        if re.match(r"^:[\+\-]*\d+$", line) is None:
            raise RedisException(f"unexpected response: {line}")
        return int(line[1:])

    def read_string(self) -> Optional[str]:
        line = self._fp.readline().rstrip("\r\n")
        if line.startswith("-"):
            raise RedisException(line[1:])
        if re.match(r"^\$[\+\-]*\d+$", line) is None:
            raise RedisException(f"unexpected response: {line}")
        size = int(line[1:])
        if size <= 0:
            return None
        return self._fp.readline().rstrip("\r\n")

    def read_ok(self) -> bool:
        line = self._fp.readline().rstrip("\r\n")
        if line.startswith("-"):
            raise RedisException(line[1:])
        if line != "+OK":
            raise RedisException(f"unexpected response: {line}")
        return True
