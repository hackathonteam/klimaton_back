from fastapi import FastAPI, File, UploadFile, Body
from typing import Any, Coroutine, List, Dict, Optional, Tuple
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
import pandas as pd
from datetime import datetime as dt
import json
from pydantic import BaseModel
from pandas import io

from data_processing import preprocess
import trucks_data
import osmapi


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


@app.get('/containers')
async def get_all_containers():
    with open("./message.txt") as f:
        data = json.load(f)
    with open("./data/location_data.json") as f:
        location_data = json.load(f)
    for v in location_data:
        k = v['name']
        long = v['longtitude']
        lat = v['latitude']
        data[k]['longtitude'] = long
        data[k]['lat'] = lat
    return data


@app.get('/containers/graphs/{id}/{graph_name}')
async def get_graph_by_id_graph_name(graph_name: str, id: str):
    data = {"name": graph_name, "title": "Pobrana woda na przestrzeni czasu", "data": [
        {'date': "2019-09", 'water': 10, 'sewage': 15}, {'date': "2019-10", 'water': 11, 'sewage': 20}, {'date': "2019-11", 'water': 14, 'sewage': 20}, {'date': "2019-12", 'water': 15, 'sewage': 20}]}
    return data


@ app.post("/upload", status_code=200)
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
        with open("data/trucks_data.json","r") as f:
            truck_data =json.load(f)
    except Exception as e:
        truck_data = {}
        with open("data/trucks_data.json","w+") as f:
            json.dump(truck_data, f)

    data2 = io.json.read_json('data/trucks_data.json')
    new_data_frame_final = trucks_data.get_truck_data().join(data2).transpose().to_json()
    # {"id": str, "comment": str, "checked": bool}

    essa = [{**value,  **{"id":key}} for key, value in json.loads(new_data_frame_final).items()]
    return essa

@app.post('/location')
async def location(loc_str: str, body: Optional[List[str]] = Body(None)):
    loc_list: List[osmapi.Location_Type] = []
    loc_list_await: List[Coroutine[Any, Any,osmapi.Location_Type]] = []
    if body:
        for elem in body:
            ret = osmapi.getLocation(elem)
            if ret:
                loc_list_await.append(ret)
            else:
                loc_list.append((52.535203, 17.6470747, elem))
        loc_list.extend([await ret for ret in loc_list_await if ret is not None])

    return [{"latitude": x[0], "longtitude": x[1], "name": x[2]} for x in loc_list if x is not None]
