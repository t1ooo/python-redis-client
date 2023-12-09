from io import StringIO
import random
from client import _ResponseReader, RedisClient, RedisException
import pytest


# client connect
def test_connect():
    client = RedisClient()
    client.close()


# client connect
def test_client_random_ops():
    """Sequentially calling random operations should not cause an error."""
    client = RedisClient()
    ops = [
        lambda: client.get("1"),
        lambda: client.set("1", "2"),
        lambda: client.delete("1"),
        lambda: client.exists("1"),
    ]
    for _ in range(10):
        op = random.choice(ops)
        op()
    client.close()


# client set
@pytest.mark.parametrize(
    "input_data, expected",
    [
        (("1", "2"), True),
        (("3", "4"), True),
        (("5", "6"), True),
        (("7", "8"), True),
    ],
)
def test_client_set(input_data, expected):
    client = RedisClient()
    assert client.set(*input_data) == True
    client.close()


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        (("1", ""), RedisException, "ERR.*"),
        (("", "4"), RedisException, "ERR.*"),
        (("", ""), RedisException, "ERR.*"),
    ],
)
def test_client_set_error(input_data, error, match):
    client = RedisClient()
    with pytest.raises(error, match=match):
        client.set(*input_data)
    client.close()


# client get
@pytest.mark.parametrize(
    "input_data, expected",
    [
        (("1", "2"), "2"),
        (("3", "4"), "4"),
        (("5", "6"), "6"),
        (("7", "8"), "8"),
        (("2345345635345", "8"), None),
        (("3576356735673", "8"), None),
        (("4657857356735", "8"), None),
        (("5457357362465", "8"), None),
    ],
)
def test_client_get(input_data, expected):
    client = RedisClient()
    if expected is not None:
        client.set(*input_data)
    assert client.get(input_data[0]) == expected
    client.close()


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        ("", RedisException, "ERR.*"),
    ],
)
def test_client_get_error(input_data, error, match):
    client = RedisClient()
    with pytest.raises(error, match=match):
        client.get(input_data)
    client.close()


# client delete
@pytest.mark.parametrize(
    "input_data, expected",
    [
        (("1", "2"), 1),
        (("3", "4"), 1),
        (("5", "6"), 1),
        (("7", "8"), 1),
        (("2345345635345", "8"), 0),
        (("3576356735673", "8"), 0),
        (("4657857356735", "8"), 0),
        (("5457357362465", "8"), 0),
    ],
)
def test_client_delete(input_data, expected):
    client = RedisClient()
    if expected != 0:
        client.set(*input_data)
    assert client.delete(input_data[0]) == expected
    client.close()


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        ("", RedisException, "ERR.*"),
    ],
)
def test_client_delete_error(input_data, error, match):
    client = RedisClient()
    with pytest.raises(error, match=match):
        client.delete(input_data)
    client.close()


# client exists
@pytest.mark.parametrize(
    "input_data, expected",
    [
        (("1", "2"), 1),
        (("3", "4"), 1),
        (("5", "6"), 1),
        (("7", "8"), 1),
        (("2345345635345", "8"), 0),
        (("3576356735673", "8"), 0),
        (("4657857356735", "8"), 0),
        (("5457357362465", "8"), 0),
    ],
)
def test_client_exists(input_data, expected):
    client = RedisClient()
    if expected != 0:
        client.set(*input_data)
    assert client.exists(input_data[0]) == expected
    client.close()


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        ("", RedisException, "ERR.*"),
    ],
)
def test_client_exists_error(input_data, error, match):
    client = RedisClient()
    with pytest.raises(error, match=match):
        client.exists(input_data)
    client.close()


# response reader read string
@pytest.mark.parametrize(
    "input_data, expected",
    [
        ("$1\r\na", "a"),
        ("$3\r\nabc", "abc"),
        ("$0", None),
        ("$-1", None),
    ],
)
def test_response_reader_read_string(input_data, expected):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    assert rr.read_string() == expected


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        (":1", RedisException, None),
        ("$a", RedisException, None),
        ("-ERR", RedisException, None),
    ],
)
def test_response_reader_read_string_error(input_data, error, match):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    with pytest.raises(error, match=match):
        rr.read_string()


# response reader read integer
@pytest.mark.parametrize(
    "input_data, expected",
    [
        (":1", 1),
        (":+1", 1),
        (":-1", -1),
    ],
)
def test_response_reader_read_integer(input_data, expected):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    assert rr.read_integer() == expected


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        ("$1", RedisException, None),
        ("$:a", RedisException, None),
        ("-ERR", RedisException, None),
    ],
)
def test_response_reader_read_integer_error(input_data, error, match):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    with pytest.raises(error, match=match):
        rr.read_integer()


# response reader read ok
@pytest.mark.parametrize(
    "input_data, expected",
    [
        ("+OK", True),
    ],
)
def test_response_reader_read_ok(input_data, expected):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    assert rr.read_ok() == expected


@pytest.mark.parametrize(
    "input_data, error, match",
    [
        ("-ERR", RedisException, None),
    ],
)
def test_response_reader_read_ok_error(input_data, error, match):
    s = StringIO()
    s.write(input_data + "\r\n")
    s.seek(0)
    rr = _ResponseReader(s)
    with pytest.raises(error, match=match):
        rr.read_ok()
