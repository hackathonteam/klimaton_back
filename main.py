from fastapi import FastAPI, File, UploadFile
from typing import List, Dict, Optional
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from data_processing import preprocess
import pandas as pd

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
    dane = [
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
    return {'data':[{"latitude": l1 ,"longtitude": l2,"name": name } for l1,l2,name in dane]}


@app.post('/calc')
async def calc_all():
    data: pd.DataFrame = preprocess()
    return "hej"

@app.post("/upload")
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

# declaredSewage
# realSewage
# waterConsumption
# companies
# meters
# sewageReception
# residents
# containers


    files = (
        ('declared_sewage',declaredSewage),
        ('real_sewage',realSewage),
        ('water_consumption',waterConsumption),
        ('companies',companies),
        ('meters',meters),
        ('sewage_reception',sewageReception),
        ('residents',residents),
        ('containers',containers))


    for name, file in files:
        if (file):
            with open(f"data/{name}.xlsx","wb+") as buffer:
                buffer.write(file)

    ret = RedirectResponse(url='/calc')
    return ret
