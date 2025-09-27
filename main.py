from fastapi import FastAPI, HTTPException, Query
from database import obtenerRecibosHoy, obtenerRecibosConIntervalo, obtenerRecibosConIntervaloYContribuyente, \
    obtenerTotalesYDescuentos, obtenerDespliegueTotales, obtenerCedulasConIntervalo, \
    obtenerCedulasConIntervaloYContribuyente, yymmdd_to_human, build_pdf, _attachment_headers
from dotenv import load_dotenv
import os
from fastapi.responses import StreamingResponse
from io import BytesIO


load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

app = FastAPI()

@app.get("/recibos/totales/despliegue")
async def obtenerSumaTotalesDespliegue(
    desde: str = Query(..., description="Fecha de inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha de fin (yymmdd)")
):
    totales = obtenerDespliegueTotales(desde, hasta)
    return totales
    
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

#CEDULAS

@app.get("/cedulas")
async def buscarCedulasIntervalo(
    desde: str = Query(..., description="Fecha de inicio del intervalo (yymmdd)"),
    hasta: str = Query(..., description="Fecha de fin del intervalo (yymmdd)")
):
    cedulas = obtenerCedulasConIntervalo(desde, hasta)
    if cedulas:
        return cedulas
    raise HTTPException(status_code=404, detail="No se encontraron cedulas en ese intervalo")

@app.get("/cedulas/filtrar")
async def buscarCedulasContribuyenteIntervalo(
    desde: str = Query(...),
    hasta: str = Query(...),
    contribuyente: str = Query(...)
):
    recibos = obtenerCedulasConIntervaloYContribuyente(desde, hasta, contribuyente)
    if recibos:
        return recibos
    raise HTTPException(status_code=404, detail="No se encontraron recibos con ese contribuyente en ese intervalo")


# -----------------------
# Endpoint: Recibos -> PDF
# -----------------------
@app.get("/recibos/reporte")
async def reporte_recibos(
    desde: str = Query(..., description="Fecha inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha fin (yymmdd)"),
    contribuyente: str | None = Query(default=None, description="Filtro opcional por contribuyente"),
):
    # Datos
    if contribuyente and contribuyente.strip():
        data = obtenerRecibosConIntervaloYContribuyente(desde, hasta, contribuyente.strip())
    else:
        data = obtenerRecibosConIntervalo(desde, hasta)

    # Armar filas
    headers = ["Recibo", "Fecha", "Contribuyente", "Concepto", "Neto", "Descuento"]
    rows = []
    for r in data:
        rows.append([
            str(r.get("recibo", "")),
            yymmdd_to_human(str(r.get("fecha", ""))),
            str(r.get("contribuyente", "")),
            str(r.get("concepto", "")),
            f"${float(r.get('neto', 0)):,.2f}",
            f"${float(r.get('descuento', 0)):,.2f}",
        ])

    title = "Reporte de Recibos"
    rango = f"{yymmdd_to_human(desde)} a {yymmdd_to_human(hasta)}"
    sub = f"Rango: {rango}" + (f" — Contribuyente: {contribuyente.strip()}" if contribuyente else "")

    pdf_bytes = build_pdf(title, sub, headers, rows)
    fname = f"recibos_{desde}-{hasta}" + (f"_{contribuyente.strip().upper().replace(' ', '_')}" if contribuyente else "") + ".pdf"
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf", headers=_attachment_headers(fname))

# -----------------------
# Endpoint: Cédulas -> PDF
# -----------------------
@app.get("/cedulas/reporte")
async def reporte_cedulas(
    desde: str = Query(..., description="Fecha inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha fin (yymmdd)"),
    contribuyente: str | None = Query(default=None, description="Filtro opcional por contribuyente"),
):
    # Datos
    if contribuyente and contribuyente.strip():
        data = obtenerCedulasConIntervaloYContribuyente(desde, hasta, contribuyente.strip())
    else:
        data = obtenerCedulasConIntervalo(desde, hasta)

    # Armar filas
    headers = ["Folio", "Fecha", "Contribuyente", "Motivo", "Dirección"]
    rows = []
    for r in data:
        rows.append([
            str(r.get("folio", "")),
            yymmdd_to_human(str(r.get("fecham", ""))),
            str(r.get("contribuyente", "")),
            str(r.get("motivo", "")),
            str(r.get("direccion", "") or ""),
        ])

    title = "Reporte de Cédulas"
    rango = f"{yymmdd_to_human(desde)} a {yymmdd_to_human(hasta)}"
    sub = f"Rango: {rango}" + (f" — Contribuyente: {contribuyente.strip()}" if contribuyente else "")

    pdf_bytes = build_pdf(title, sub, headers, rows)
    fname = f"cedulas_{desde}-{hasta}" + (f"_{contribuyente.strip().upper().replace(' ', '_')}" if contribuyente else "") + ".pdf"
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf", headers=_attachment_headers(fname))
