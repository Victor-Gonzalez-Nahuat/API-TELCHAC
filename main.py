from fastapi import FastAPI, HTTPException, Query
from database import obtenerRecibosHoy, obtenerRecibosConIntervalo, obtenerRecibosConIntervaloYContribuyente, obtenerTotalesYDescuentos
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

app = FastAPI()

@app.get("/recibos/totales")
async def obtenerSumaTotalesYDescuentos(
    desde: str = Query(..., description="Fecha de inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha de fin (yymmdd)"),
    contribuyente: str = Query(None, description="(Opcional) Filtro por contribuyente")
):
    totales = obtenerTotalesYDescuentos(desde, hasta, contribuyente)
    return totales

@app.get("/recibos/filtrar")
async def buscarRecibosContribuyenteIntervalo(
    desde: str = Query(...),
    hasta: str = Query(...),
    contribuyente: str = Query(...)
):
    recibos = obtenerRecibosConIntervaloYContribuyente(desde, hasta, contribuyente)
    if recibos:
        return recibos
    raise HTTPException(status_code=404, detail="No se encontraron recibos con ese contribuyente en ese intervalo")

@app.get("/recibos")
async def buscarRecibosIntervalo(
    desde: str = Query(..., description="Fecha de inicio del intervalo (yymmdd)"),
    hasta: str = Query(..., description="Fecha de fin del intervalo (yymmdd)")
):
    recibos = obtenerRecibosConIntervalo(desde, hasta)
    if recibos:
        return recibos
    raise HTTPException(status_code=404, detail="No se encontraron recibos en ese intervalo")

@app.get("/recibos/hoy")
async def buscarRecibosHoy():
    ofertas = obtenerRecibosHoy()
    if ofertas:
        return ofertas
    raise HTTPException(status_code=404, detail="No se encontraron ofertas")

