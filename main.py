import json
from datetime import datetime as dt
from typing import Any, Coroutine, List, Dict, Optional

from fastapi import FastAPI, File, Body
from fastapi.middleware.cors import CORSMiddleware
from pandas import io
from pydantic import BaseModel

import citizens_data
import osmapi
import trucks_data

app = FastAPI()

origins = [
    'http://localhost:4200',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/city/details')
async def get_city_details():
    return {'id': 'Gniezno', 'ratio': 0.89, 'mean': {'water_means': []}}


@app.get('/containers')
async def get_all_containers():
    # data = citizens_data.create_map_datapoints(citizens_data.generate_df())
    data = citizens_data.create_map_datapoints(citizens_data.generate_map_datapoints_df())

    with open("./data/location_data.json") as f:
        location_data = json.load(f)

    for v in location_data:
        k = v['name']
        if k in data:
            long = v['longtitude']
            lat = v['latitude']
            data[k]['longtitude'] = long
            data[k]['latitude'] = lat

    data = [{**{'id': key}, **value} for key, value in data.items() if
            value is not None and 'longtitude' in value and value['longtitude'] is not None and 'latitude' in value and
            value['latitude'] is not None]
    data.sort(key=lambda x: x['st_oddanej_do_pobranej'])
    return data


@app.get('/containers/default')
async def get_containers_default():
    return await get_containers_higher_than_percent(str(70))


@app.get('/containers/{precent}')
async def get_containers_higher_than_percent(percent: str):
    percent_i = int(percent)
    data = list(filter(
        lambda x: x and x != {} and 'st_oddanej_do_pobranej' in x and x['st_oddanej_do_pobranej'] < percent_i * 0.01,
        await get_all_containers()))
    data.sort(key=lambda x: x['st_oddanej_do_pobranej'])
    return data


@app.get('containers/details/{id}')
async def get_all_data_by_id(id: str):
    data = await get_all_containers()
    data = list(filter(lambda x: x['id'] == id, data))[0]
    return data


class Essa(BaseModel):
    graph_name: str
    id: str


@app.post('/containers/graphs/')
async def get_graph_by_id_graph_name(va: Essa = Body(...)):
    graph_name = va.graph_name
    id = va.id
    if graph_name == 'quotient_timeseries':
        data = citizens_data.graph_quotient_timeseries(citizens_data.generate_quotient_timeseries_df(), id)
    else:
        data = citizens_data.graph_amount_timeseries(citizens_data.generate_quotient_timeseries_df(), id)
    return data


@app.post("/upload", status_code=200)
async def create_upload_file(
        declaredSewage: bytes = File(None),
        realSewage: bytes = File(None),
        waterConsumption: bytes = File(None),
        companies: bytes = File(None),
        meters: bytes = File(None),
        sewageReception: bytes = File(None),
        residents: bytes = File(None),
        containers: bytes = File(None)
):
    files = [
        (declaredSewage, "declaredSewage"),
        (realSewage, 'realSewage'),
        (waterConsumption, 'waterConsumption'),
        (companies, 'companies'),
        (meters, 'meters'),
        (sewageReception, 'sewageReception'),
        (residents, 'residents'),
        (containers, 'containers')]

    json_data: Dict[str, Any]
    try:
        with open('data/modification.json', "r+") as modifictaion_file:
            json_data = json.load(modifictaion_file)
    except Exception as e:
        json_data = {}

    current_date = dt.now().strftime("%d-%m-%Y %H:%M")
    for file, json_name in files:
        if (file):
            json_data[json_name] = current_date
            with open(f"data/{json_name}.xlsx", "wb+") as buffer:
                buffer.write(file)

    with open('data/modification.json', 'w+') as modifictaion_file:
        json.dump(json_data, modifictaion_file)

    return None


@app.get('/last_modified')
async def last_uploaded():
    try:
        with open(f"data/modification.json") as modifictaion_file:
            return json.load(modifictaion_file)
    except Exception:
        return {}


# ciezaruwy

@app.get('/trucks')
async def get_all_trucks_info():
    # trucks_data: Dict[str, Any]
    try:
        with open("data/trucks_data.json", "r") as f:
            truck_data = json.load(f)
    except Exception as e:
        truck_data = {}
        with open("data/trucks_data.json", "w+") as f:
            json.dump(truck_data, f)

    data2 = io.json.read_json('data/trucks_data.json')
    new_data_frame_final = trucks_data.get_truck_data().join(data2)  # .to_json()
    # return new_data_frame_final
    # {"id": str, "comment": str, "checked": bool}

    essa = [{**value} for key, value in json.loads(new_data_frame_final.to_json()).items()]
    return essa


@app.post('/location')
async def location(loc_str: str, body: Optional[List[str]] = Body(None)):
    loc_list: List[osmapi.Location_Type] = []
    loc_list_await: List[Coroutine[Any, Any, osmapi.Location_Type]] = []
    if body:
        for elem in body:
            ret = osmapi.getLocation(elem)
            if ret:
                loc_list_await.append(ret)
            else:
                loc_list.append((52.535203, 17.6470747, elem))
        loc_list.extend([await ret for ret in loc_list_await if ret is not None])

    return [{"latitude": x[0], "longtitude": x[1], "name": x[2]} for x in loc_list if x is not None]


class Switch(BaseModel):
    id: str
    value: str = ""


async def update_location_data(body: Switch, inner_value: str):
    data = {}
    try:
        with open('data/s_c_data.json') as f:
            data = json.load(f)
    except Exception as e:
        with open('data/s_c_data.json', 'w+') as f:
            json.dump(data, f)

    if body.id not in data:
        data[body.id] = {}

    data[body.id][inner_value] = body.value
    with open('data/s_c_data.json', 'w+') as f:
        json.dump(data, f)


async def get_location_data(id: str, inner_value: str):
    ret = {'id': id, 'value': None}
    try:
        with open('data/s_c_data.json') as f:
            data = json.load(f)
            if id in data:
                ret = data[id][inner_value]
    except Exception as e:
        with open('data/s_c_data.json', 'w+') as f:
            json.dump({}, f)

    return ret


# typical crud on notatki and switch zalezne od adresu
# {id: adress, value: value}
@app.put('/switch')
async def make_a_switch(body: Switch = Body(...)):
    await update_location_data(body, 'switch')


@app.get('/switch/{id}')
async def get_value_switch(id: str):
    return await get_location_data(id, 'switch')


@app.put('/comment')
async def make_a_comment_location(body: Switch = Body(...)):
    await update_location_data(body, 'comment')


@app.get('/comment/{id}')
async def get_value_comment(id: str):
    return await get_location_data(id, 'comment')


# ciezaruwa

async def update_data_truck(body: Switch, inner_value: str):
    data = {}
    try:
        with open('data/trucks_data.json') as f:
            data = json.load(f)
    except Exception as e:
        with open('data/trucks_data.json', 'w+') as f:
            json.dump(data, f)

    # data_payload = json.loads(body)

    if body.id not in data:
        data[body.id] = {}

    data[body.id][inner_value] = body.value
    with open('data/trucks_data.json', 'w+') as f:
        json.dump(data, f)


async def get_data_truck(id: str, inner_value: str):
    ret = {'id': id, 'value': None}
    try:
        with open('data/trucks_data.json') as f:
            data = json.load(f)
            if id in data:
                ret = data[id][inner_value]
    except Exception as e:
        with open('data/trucks_data.json', 'w+') as f:
            json.dump({}, f)

    return ret


@app.put('/truck/switch')
async def make_a_switch_for_truck(body: Switch = Body(...)):
    await update_data_truck(body, 'switch')
    return ''


@app.get('/truck/switch/{id}')
async def get_value_switch_for_truck(id: str):
    return await get_data_truck(id, 'switch')


@app.put('/truck/comment')
async def make_a_comment_for_truck(body: Switch = Body(...)):
    await update_data_truck(body, 'comment')
    return ''


@app.get('/truck/comment/{id}')
async def get_value_comment_for_truck(id: str):
    return await get_data_truck(id, 'comment')
