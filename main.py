from typing import Optional
from fastapi import FastAPI

from data import bdd

import uvicorn

app = FastAPI()

@app.get("/")
def index():
    return {"Hello":"World"}


# infos d'un restaurant à partir de son id
@app.get("/api/resto/{id}", tags=['Informations'])
async def resto_id(id):
    return bdd.info_resto_par_id(id)

#liste des noms de restaurants à partir du type de cuisine    
@app.get("/api/type/{typeresto}", tags=['Informations'])
async def resto_type(typeresto):
    return bdd.nom_resto_par_type(typeresto)

#nombre d'inspection d'un restaurant à partir de son id restaurant
@app.get("/api/inspection/{id}", tags=['Informations'])
async def nb_inspection(id):
    return bdd.nb_inspection(id)

#noms des 10 premiers restaurants d'un grade donné
@app.get("/api/grade/{grade}", tags=['Grade 10 premiers elements'])
async def resto_grade(grade):
    return bdd.nom_grade(grade)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8800)




