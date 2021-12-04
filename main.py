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
