from fastapi import FastAPI, HTTPException, Query
from reportlab.lib import colors
from reportlab.lib.units import mm

from database import obtenerRecibosHoy, obtenerRecibosConIntervalo, obtenerRecibosConIntervaloYContribuyente, \
    obtenerTotalesYDescuentos, obtenerDespliegueTotales, obtenerCedulasConIntervalo, \
    obtenerCedulasConIntervaloYContribuyente, yymmdd_to_human,_attachment_headers, build_pdf_advanced
from dotenv import load_dotenv
import os
from fastapi.responses import StreamingResponse
from io import BytesIO
import pandas as pd


load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

ENV = os.getenv("ENV", "DEV")

if ENV == "PROD":
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
else:
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
    # 1) Traer datos
    if contribuyente and contribuyente.strip():
        data = obtenerRecibosConIntervaloYContribuyente(desde, hasta, contribuyente.strip())
    else:
        data = obtenerRecibosConIntervalo(desde, hasta)

    # 2) Ordenar por fecha desc (por si algún día cambia el SQL)
    try:
        data.sort(key=lambda r: int(r.get("fecha", 0)), reverse=True)
    except Exception:
        pass

    # 3) Armar filas: Recibo | Fecha | Contribuyente | Concepto | Neto | Descuento
    headers = ["Recibo", "Fecha", "Contribuyente", "Concepto", "Neto", "Descuento", "% Desc.", "Forma Pago"]
    rows = []
    total_neto = 0.0
    total_desc = 0.0
    for r in data:
        neto = float(r.get("neto", 0) or 0)
        desc = float(r.get("descuento", 0) or 0)
        porcentaje_desc = f"{r.get("porcentaje_descuento", 0)}%"
        forma_pago = r.get("forma_pago", "N/A")
        total_neto += neto
        total_desc += desc
        rows.append([
            r.get("recibo", ""),
            yymmdd_to_human(str(r.get("fecha", ""))),
            r.get("contribuyente", ""),
            r.get("concepto", ""),
            f"${neto:,.2f}",
            f"${desc:,.2f}",
            porcentaje_desc,
            forma_pago
        ])

    # 4) Fila de totales (si hay datos)
    if rows:
        rows.append([
            "", "", "", "Totales:",
            f"${total_neto:,.2f}",
            f"${total_desc:,.2f}",
        ])

    title = "Reporte de Recibos"
    rango = f"{yymmdd_to_human(desde)} a {yymmdd_to_human(hasta)}"
    sub = f"Rango: {rango}" + (f" — Contribuyente: {contribuyente.strip()}" if contribuyente else "")

    # Col widths sugeridos (A4 landscape)
    col_widths = [
        22*mm,  # Recibo
        22*mm,  # Fecha
        55*mm,  # Contribuyente
        65*mm,  # Concepto
        25*mm,  # Neto
        25*mm,  # Descuento
    ]

    # Alinear Neto/Descuento a la derecha y poner la última fila (totales) en bold
    last_row_index = len(rows)  # en la tabla real será +1 por el header
    extra_styles = [
        ("ALIGN", (4,1), (5,-1), "RIGHT"),
        ("FONTNAME", (0,last_row_index), (-1,last_row_index), "Helvetica-Bold"),
        ("BACKGROUND", (0,last_row_index), (-1,last_row_index), colors.HexColor("#BC955C")), # Fondo Dorado para totales
        ("TEXTCOLOR", (0,last_row_index), (-1,last_row_index), colors.white), # Texto blanco para que resalte
    ]

    pdf_bytes = build_pdf_advanced(
        title, sub, headers, rows,
        col_widths=col_widths,
        landscape_mode=True,
        extra_styles=extra_styles
    )

    fname = f"recibos_{desde}-{hasta}" + (f"_{contribuyente.strip().upper().replace(' ', '_')}" if contribuyente else "") + ".pdf"
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf", headers=_attachment_headers(fname))

@app.get("/recibos/excel")
async def reporte_recibos_excel(
    desde: str = Query(..., description="Fecha inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha fin (yymmdd)"),
    contribuyente: str | None = Query(default=None, description="Filtro opcional por contribuyente"),
):
    # 1) Reutilizamos tu lógica de obtención de datos
    if contribuyente and contribuyente.strip():
        data = obtenerRecibosConIntervaloYContribuyente(desde, hasta, contribuyente.strip())
    else:
        data = obtenerRecibosConIntervalo(desde, hasta)

    if not data:
        raise HTTPException(status_code=404, detail="No hay datos para exportar")

    # 2) Formatear los datos para que el Excel se vea "humano"
    filas_excel = []
    for r in data:
        filas_excel.append({
            "Folio": r.get("recibo"),
            "Fecha": yymmdd_to_human(str(r.get("fecha"))),
            "Contribuyente": r.get("contribuyente"),
            "Concepto": r.get("concepto"),
            "Monto Neto": float(r.get("neto") or 0),
            "Descuento": float(r.get("descuento") or 0),
            "% Desc": f"{r.get('porcentaje_descuento')}%",
            "Forma de Pago": r.get("forma_pago")
        })

    # 3) Crear el Excel usando Pandas
    df = pd.DataFrame(filas_excel)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Recibos')
        
        # Ajuste básico de ancho de columnas (opcional pero se ve pro)
        worksheet = writer.sheets['Recibos']
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = max_len

    output.seek(0)

    # 4) Nombre del archivo y retorno
    fname = f"recibos_{desde}_{hasta}.xlsx"
    return StreamingResponse(
        output, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=_attachment_headers(fname)
    )



# ---- ENDPOINT mejorado: CÉDULAS -> PDF organizado ----
@app.get("/cedulas/reporte")
async def reporte_cedulas(
    desde: str = Query(..., description="Fecha inicio (yymmdd)"),
    hasta: str = Query(..., description="Fecha fin (yymmdd)"),
    contribuyente: str | None = Query(default=None, description="Filtro opcional por contribuyente"),
):
    # 1) Trae datos
    if contribuyente and contribuyente.strip():
        data = obtenerCedulasConIntervaloYContribuyente(desde, hasta, contribuyente.strip())
    else:
        data = obtenerCedulasConIntervalo(desde, hasta)

    # 2) Ordena por fecha desc (por si el SQL cambia)
    try:
        data.sort(key=lambda r: int(r.get("fecham", 0)), reverse=True)
    except Exception:
        pass

    # 3) Arma filas en el orden: Folio | Fecha | Contribuyente | Motivo | Dirección
    headers = ["Folio", "Fecha", "Folio Elec.", "Contribuyente", "Motivo", "Dirección", "Importe", "Recibo", "Fecha Recibo"]
    rows = []
    for r in data:
        importe = float(r.get("precio_unitario")) * float(r.get("cantidad"))
        rows.append([
            r.get("folio", ""),
            yymmdd_to_human(str(r.get("fecham", ""))),
            r.get("folio_electronico"),
            r.get("contribuyente", ""),
            r.get("motivo", ""),
            r.get("direccion", "") or "",
            f"${importe}",
            r.get("recibo_teso") if r.get("recibo_teso") != None else "Sin recibo",
            yymmdd_to_human(str(r.get("fecha_rteso"))) if r.get("recibo_teso") != None else "Sin recibo"
        ])

    title = "Reporte de Cédulas"
    rango = f"{yymmdd_to_human(desde)} a {yymmdd_to_human(hasta)}"
    sub = f"Rango: {rango}" + (f" — Contribuyente: {contribuyente.strip()}" if contribuyente else "")

    # 4) Col widths pensadas para paisaje A4 (ajústalas si lo ves apretado)
    col_widths = [22*mm, 22*mm, 22*mm, 50*mm, 50*mm, 50*mm, 22*mm, 22*mm, 22*mm]

    pdf_bytes = build_pdf_advanced(title, sub, headers, rows, col_widths=col_widths, landscape_mode=True)
    fname = f"cedulas_{desde}-{hasta}" + (f"_{contribuyente.strip().upper().replace(' ', '_')}" if contribuyente else "") + ".pdf"
    return StreamingResponse(BytesIO(pdf_bytes), media_type="application/pdf", headers=_attachment_headers(fname))