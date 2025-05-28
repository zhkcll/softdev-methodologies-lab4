import pytest
from .datastore import Datastore


@pytest.fixture
def ds():
    return Datastore()


# ---------- String Tests ----------


def test_str_set_get(ds):
    assert ds.strset(b'key', b'value') is True
    assert ds.strget(b'key') == b'value'
    assert ds.strget(b'nonexistent') is None


def test_str_overwrite(ds):
    ds.strset(b'k', b'1')
    ds.strset(b'k', b'2')
    assert ds.strget(b'k') == b'2'


# ---------- List Tests ----------


def test_lpush_rpush_lrange(ds):
    assert ds.lpush(b'list', b'a', b'b') == 2
    assert ds.rpush(b'list', b'c') == 3
    assert ds.lrange(b'list', 0, -1) == [b'b', b'a', b'c']
    assert ds.lrange(b'list', 1, 1) == [b'a']
    assert ds.lrange(b'list', 10, 20) == []


def test_lpush_create_new(ds):
    ds.lpush(b'newlist', b'x')
    assert ds.lrange(b'newlist', 0, -1) == [b'x']


# ---------- Set Tests ----------


def test_sadd_smembers(ds):
    assert ds.sadd(b'set', b'a', b'b') == 2
    assert ds.sadd(b'set', b'b', b'c') == 1
    members = ds.smembers(b'set')
    assert members == {b'a', b'b', b'c'}


def test_smembers_empty(ds):
    assert ds.smembers(b'empty') == set()


# ---------- Hash Tests ----------


def test_hset_hget(ds):
    assert ds.hset(b'hash', b'f1', b'v1') == 1
    assert ds.hset(b'hash', b'f1', b'v2') == 0
    assert ds.hget(b'hash', b'f1') == b'v2'
    assert ds.hget(b'hash', b'f2') is None


def test_hgetall(ds):
    ds.hset(b'h', b'k1', b'v1')
    ds.hset(b'h', b'k2', b'v2')
    assert ds.hgetall(b'h') == {b'k1': b'v1', b'k2': b'v2'}


# ---------- Sorted Set Tests ----------


def test_zadd_zrange(ds):
    mapping = {b'a': 1.0, b'b': 0.5}
    assert ds.zadd(b'zset', mapping) == 2
    assert ds.zadd(b'zset', {b'a': 2.0}) == 0
    assert ds.zrange(b'zset', 0, -1) == [b'b', b'a']


def test_zrange_with_score(ds):
    ds.zadd(b'z', {b'x': 1.5, b'y': 0.1})
    assert ds.zrange(b'z', 0, -1, withscores=True) == [(b'y', 0.1), (b'x', 1.5)]


# ---------- Delete ----------


def test_delete_existing(ds):
    ds.strset(b'dkey', b'dval')
    assert ds.delete(b'dkey') == 1
    assert ds.strget(b'dkey') is None


def test_delete_nonexistent(ds):
    assert ds.delete(b'missing') == 0
