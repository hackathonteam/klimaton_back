from fastapi import FastAPI, File, UploadFile
from typing import List, Dict, Optional
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

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
    return "essa z calc_all"

@app.post("/upload")
async def create_upload_file(
    file1: bytes = File(None),
    file2: bytes = File(None),
    file3: bytes = File(None),
    file4: bytes = File(None),
    file5: bytes = File(None),
    file6: bytes = File(None),
    file7: bytes = File(None),
    file8: bytes = File(None)
    ):

    files = (
        ('nazwa1', file1),
        ('nazwa2', file2),
        ('nazwa3', file3),
        ('nazwa4', file4),
        ('nazwa5', file5),
        ('nazwa6', file6),
        ('nazwa7', file7),
        ('nazwa8', file8))


    for name, file in files:
        if (file):
            with open(f"data/{name}.xlsx","wb+") as buffer:
                buffer.write(file)

    ret = RedirectResponse(url='/calc')
    return ret
