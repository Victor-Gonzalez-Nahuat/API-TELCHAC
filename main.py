from fastapi import FastAPI, HTTPException
from database import obtenerLosPrimerosProductos, obtenerProductosPorCodigo, obtenerProductosPorNombre, obtenerOfertas
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

app = FastAPI()

@app.get("/ofertas/")
async def buscar_ofertas():
    ofertas = obtenerOfertas()
    if ofertas:
        return ofertas
    raise HTTPException(status_code=404, detail="No se encontraron ofertas")

@app.get("/producto/{codigo}")
async def buscar_producto(codigo: str):
    producto = obtenerProductosPorCodigo(codigo)
    if producto:
        return producto
    raise HTTPException(status_code=404, detail="Producto no encontrado")

@app.get("/productos/")
async def obtener_productos(limit: int = 10):
    productos = obtenerLosPrimerosProductos(limit)
    if productos:
        return productos
    raise HTTPException(status_code=404, detail="No se encontraron productos")

@app.get("/producto/nombre/{nombre}")
async def obtener_codigo(nombre: str):
    codigo = obtenerProductosPorNombre(nombre)
    if codigo:
        return codigo
    raise HTTPException(status_code=404, detail="Código no encontrado")
