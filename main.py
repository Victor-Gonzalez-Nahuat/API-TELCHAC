from fastapi import FastAPI, HTTPException
from database import obtenerRecibosHoy
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

app = FastAPI()

@app.get("/recibos/hoy")
async def buscar_ofertas():
    ofertas = obtenerRecibosHoy()
    if ofertas:
        return ofertas
    raise HTTPException(status_code=404, detail="No se encontraron ofertas")

