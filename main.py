from fastapi import FastAPI, File, UploadFile, Body
from typing import Any, Coroutine, List, Dict, Optional, Tuple
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
import pandas as pd
from datetime import datetime as dt
import json
from pydantic import BaseModel

from data_processing import preprocess
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


@app.get('/location')
async def mock_location():
    ret = RedirectResponse(url='/location/mock')
    return ret


@app.get('/location/{loc_str}')
async def location(loc_str: str, body: Optional[List[str]] = Body(None)):
    loc_list: List[osmapi.location_return_type] = []
    if loc_str == "mock":
        loc_list = [
            (52.5408707, 17.6495365, 'Roosevelta 164'),
            (52.542853, 17.6092003, 'Paczkowskiego 6'),
            (52.5462825, 17.5640176, 'Kłeckoska 84'),
            (52.5526451, 17.6267169, 'Zamiejska 13'),
            (52.5389391, 17.5858096, 'Kłeckoska 96 A'),
            (52.54179935, 17.645604900089765, 'Trzemeszeńska 2F'),
            (52.5424377, 17.5756189, 'Kłeckoska 51'),
            (52.5399026, 17.6166866, 'Roosevelta 131 A'),
            (52.519692899999995, 17.574187216070108, 'Skrajna 10'),
            (52.541709, 17.563854, 'Żerniki 6'),
            (52.5184844, 17.5746475, 'Skrajna 11'),
            (52.5117585, 17.5946187, 'Wrzesińska 84'),
            (52.562723, 17.6075159, 'Pomowska 14'),
            (52.5193267, 17.5737556, 'Ludwiczaka 38'),
            (52.535203, 17.6470747, 'Grodzka 9')
        ]
    else:
        loc_list_await: List[Coroutine[Any, Any,
                                       osmapi.location_return_type]] = []
        if body:
            for elem in body:
                ret = osmapi.getLocation(elem)
                if ret:
                    loc_list_await.append(ret)
                else:
                    loc_list.append((52.535203, 17.6470747, elem))
            loc_list.extend([await ret for ret in loc_list_await])

    return [{"latitude": l1, "longtitude": l2, "name": name} for l1, l2, name in loc_list]


@app.get('/containers')
async def get_all_containers():
    with open("./message.txt") as f:
        data = json.load(f)
    return data


@app.get('/containers/graphs/{id}/{graph_name}')
async def get_graph_by_id_graph_name(graph_name: str, id: str):
    data = {"name": graph_name, "title": "Pobrana woda na przestrzeni czasu", "data": [
        {'date': "2019-09", 'value': 10}, {'date': "2019-10", 'value': 11}, {'date': "2019-11", 'value': 14}, {'date': "2019-12", 'value': 15}]}
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


@ app.get('/last_modified')
async def last_uploaded():
    try:
        with open(f"data/modification.json") as modifictaion_file:
            return json.load(modifictaion_file)
    except Exception:
        return {}

# ciezaruwy

@app.get('/trucks')
async def get_all_trucks_info():
    pass

