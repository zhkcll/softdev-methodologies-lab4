from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from node.datastore import Datastore
from protocols import DatastoreProtocol

app = FastAPI()
ds: DatastoreProtocol = Datastore()


# ------------------ STRING ------------------ #
class StringRequest(BaseModel):
    key: bytes
    value: bytes


@app.post('/string/')
async def set_string(data: StringRequest):
    ds.strset(data.key, data.value)
    return {'status': 'ok'}


@app.get('/string/')
async def get_string(key: bytes):
    val = ds.strget(key)
    if val is None:
        raise HTTPException(status_code=404, detail='Key not found')
    return {'key': key, 'value': val}


# ------------------ LIST ------------------ #
class ListPushRequest(BaseModel):
    key: bytes
    values: list[bytes]


@app.post('/list/rpush/')
async def rpush_list(data: ListPushRequest):
    ds.rpush(data.key, *data.values)
    return {
        'status': 'ok',
        'method': 'RPUSH',
        'key': data.key,
        'values': data.values,
    }


@app.post('/list/lpush/')
async def lpush_list(data: ListPushRequest):
    ds.lpush(data.key, *data.values)
    return {
        'status': 'ok',
        'method': 'LPUSH',
        'key': data.key,
        'values': data.values,
    }


@app.get('/list/')
async def get_list(key: bytes, start: int = 0, end: int = -1):
    items = ds.lrange(key, start, end)
    return {'key': key, 'range': [start, end], 'values': items}


# ------------------ SET ------------------ #
class SetRequest(BaseModel):
    key: bytes
    members: list[bytes]


@app.post('/set/')
async def add_set(data: SetRequest):
    ds.sadd(data.key, *data.members)
    return {'status': 'ok'}


@app.get('/set/')
async def get_set(key: bytes):
    return {'key': key, 'members': list(ds.smembers(key))}


# ------------------ HASH ------------------ #
class HashRequest(BaseModel):
    key: bytes
    fields: dict[bytes, bytes]


@app.post('/hash/')
async def set_hash(data: HashRequest):
    ds.hset(data.key, mapping=data.fields)
    return {'status': 'ok'}


@app.get('/hash/')
async def get_hash(key: bytes, field: bytes | None = None):
    if field:
        val = ds.hget(key, field)
        if val is None:
            raise HTTPException(status_code=404, detail='Field not found')
        return {'field': field, 'value': val}
    return ds.hgetall(key)


# ------------------ SORTED SET ------------------ #
class ZSetRequest(BaseModel):
    key: bytes
    members: dict[bytes, float]


@app.post('/zset/')
async def add_zset(data: ZSetRequest):
    ds.zadd(data.key, data.members)
    return {'status': 'ok'}


@app.get('/zset/')
async def get_zset(
    key: bytes, start: int = 0, end: int = -1, withscores: bool = True
):
    items = ds.zrange(key, start, end, withscores=withscores)
    if withscores:
        return [{'member': m, 'score': s} for m, s in items]
    else:
        return {'members': items}
