import pymysql
import os
import datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
import requests
from reportlab.platypus import Image as RLImage
from reportlab.lib.utils import ImageReader


from datetime import datetime

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = int(os.getenv('DB_PORT'))

LOGO_URL = "https://i.ibb.co/SDz9CZXS/Imagen-de-Whats-App-2025-04-22-a-las-15-46-24-f6a2c21e.jpg"


def expandir_rango_fechas(desde, hasta):
    """Ajusta el rango para que hasta incluya todo el día."""
    desde_dt = datetime.datetime.strptime(desde, '%Y-%m-%d')
    hasta_dt = datetime.datetime.strptime(hasta, '%Y-%m-%d') + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    return desde_dt, hasta_dt


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT
    )

def obtenerTotalesYDescuentos(desde_fecha, hasta_fecha, contribuyente=None):
    conn = get_connection()
    cursor = conn.cursor()

    if contribuyente:
        cursor.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN id_status = 0 THEN id_neto ELSE 0 END), 0) AS total_neto, 
                COALESCE(SUM(CASE WHEN id_status = 0 THEN id_descuento ELSE 0 END), 0) AS total_descuento,
                SUM(CASE WHEN id_status = 1 THEN 1 ELSE 0 END) AS cantidad_status_1
            FROM TEARMO01
            WHERE id_fecha BETWEEN %s AND %s
            AND id_contribuyente LIKE %s
        """, (desde_fecha, hasta_fecha, f"%{contribuyente}%"))
    else:
        cursor.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN id_status = 0 THEN id_neto ELSE 0 END), 0) AS total_neto, 
                COALESCE(SUM(CASE WHEN id_status = 0 THEN id_descuento ELSE 0 END), 0) AS total_descuento,
                SUM(CASE WHEN id_status = 1 THEN 1 ELSE 0 END) AS cantidad_status_1
            FROM TEARMO01
            WHERE id_fecha BETWEEN %s AND %s
        """, (desde_fecha, hasta_fecha))

    resultado = cursor.fetchone()
    conn.close()

    return {
        "total_neto": float(resultado[0]),
        "total_descuento": float(resultado[1]),
        "cantidad_status_1": int(resultado[2])
    }


def obtenerRecibosConIntervaloYContribuyente(desde_fecha, hasta_fecha, contribuyente):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente 
        FROM TEARMO01 
        WHERE id_fecha BETWEEN %s AND %s
        AND id_contribuyente LIKE %s
        ORDER BY id_fecha DESC
    """, (desde_fecha, hasta_fecha, f"%{contribuyente}%"))

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    recibos = [
        {
            "recibo": row[0],
            "fecha": row[1],
            "neto": row[2],
            "descuento": row[3],
            "concepto": row[4],
            "contribuyente": row[5],
        } 
        for row in resultados
    ]
    return recibos

def obtenerRecibosConIntervalo(desde_fecha, hasta_fecha):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente 
        FROM TEARMO01 
        WHERE id_fecha BETWEEN %s AND %s
        ORDER BY id_fecha DESC
    """, (desde_fecha, hasta_fecha)) 

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    recibos = [
        {
            "recibo": row[0],
            "fecha": row[1],
            "neto": row[2],
            "descuento": row[3],
            "concepto": row[4],
            "contribuyente": row[5],
        } 
        for row in resultados
    ]
    return recibos

def obtenerRecibosHoy():
    conn = get_connection()
    cursor = conn.cursor()

    fecha_hoy = datetime.datetime.today().strftime('%y%m%d')

    cursor.execute("""
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente 
        FROM TEARMO01 
        WHERE id_fecha = %s
        ORDER BY id_fecha DESC
    """, (fecha_hoy,)) 

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    recibos = [
        {
            "recibo": row[0],
            "fecha": row[1],
            "neto": row[2],
            "descuento": row[3],
            "concepto": row[4],
            "contribuyente": row[5],
        } 
        for row in resultados
    ]
    return recibos

def obtenerDespliegueTotales(desde_fecha, hasta_fecha):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            c.id_nombrecuenta,
            COALESCE(SUM(m.id_neto), 0) AS total_neto,
            COALESCE(SUM(m.id_descuento), 0) AS total_descuento
        FROM TEARMO01 m
        JOIN TEARCA01 c ON m.id_cuenta = c.id_codigoc
        WHERE m.id_fecha BETWEEN %s AND %s
        AND m.id_status = 0
        GROUP BY c.id_nombrecuenta
        ORDER BY c.id_nombrecuenta
    """, (desde_fecha, hasta_fecha))

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    despliegue = [
        {
            "cuenta": row[0],  # Ahora es el nombre de la cuenta
            "total_neto": float(row[1]),
            "total_descuento": float(row[2])
        } 
        for row in resultados
    ]
    return despliegue

#LOGICA CEDULAS

def obtenerCedulasConIntervalo(desde_fecha, hasta_fecha):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            LEFT(codigo, 6),
            motivo,
            fecham,
            contribuyente,
            direccion,
            precio_unitario,
            cantidad,
            recibo_teso,
            fecha_rteso,
            codigo
        FROM TEARMM01 WHERE fecham BETWEEN %s AND %s
        ORDER BY fecham DESC
    """, (desde_fecha, hasta_fecha))

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    cedulas = [
        {
            "folio": row[0],
            "motivo": row[1],
            "fecham": row[2],
            "contribuyente": row[3],
            "direccion": row[4],
            "precio_unitario": row[5],
            "cantidad": row[6],
            "recibo_teso": row[7],
            "fecha_rteso": row[8],
            "folio_electronico": row[9]
        }
        for row in resultados
    ]
    return cedulas


def obtenerCedulasConIntervaloYContribuyente(desde_fecha, hasta_fecha, contribuyente):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT LEFT(codigo, 6), motivo, fecham, contribuyente, direccion, precio_unitario,
            cantidad,
            recibo_teso,
            fecha_rteso,
            codigo
        FROM TEARMM01 
        WHERE fecham BETWEEN %s AND %s
        AND contribuyente LIKE %s
        ORDER BY fecham DESC
    """, (desde_fecha, hasta_fecha, f"%{contribuyente}%"))

    resultados = cursor.fetchall()
    conn.close()

    if not resultados:
        return []

    cedulas = [
        {
            "folio": row[0],
            "motivo": row[1],
            "fecham": row[2],
            "contribuyente": row[3],
            "direccion": row[4],
            "precio_unitario": row[5],
            "cantidad": row[6],
            "recibo_teso": row[7],
            "fecha_rteso": row[8],
            "folio_electronico": row[9]
        }
        for row in resultados
    ]
    return cedulas

# -----------------------
# Utilidades PDF
# -----------------------
def yymmdd_to_human(yymmdd: str) -> str:
    try:
        return datetime.strptime(yymmdd, "%y%m%d").strftime("%d-%m-%Y")
    except Exception:
        return yymmdd

def _attachment_headers(filename: str) -> dict:
    return {"Content-Disposition": f'attachment; filename="{filename}"'}

def _make_logo_flowable(url: str, max_w=35*mm, max_h=20*mm):
    """Descarga el logo y devuelve un Flowable Image ajustado a un cuadro max_w x max_h."""
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        img_bytes = BytesIO(resp.content)
        ir = ImageReader(img_bytes)
        iw, ih = ir.getSize()
        scale = min(max_w / iw, max_h / ih)
        w, h = iw * scale, ih * scale
        img_bytes.seek(0)
        return RLImage(img_bytes, width=w, height=h)
    except Exception:
        return None

def build_pdf_advanced(
    title: str,
    subtitle: str,
    headers: list[str],
    rows: list[list],
    col_widths=None,
    landscape_mode=True,
    extra_styles=None,
    logo_url: str | None = LOGO_URL,  # <--- NUEVO: usa tu constante por default
) -> bytes:
    buf = BytesIO()
    pagesize = landscape(A4) if landscape_mode else A4
    doc = SimpleDocTemplate(
        buf,
        pagesize=pagesize,
        leftMargin=14*mm, rightMargin=14*mm,
        topMargin=12*mm, bottomMargin=12*mm,
        title=title
    )
    styles = getSampleStyleSheet()
    cell_style = ParagraphStyle("cell", parent=styles["Normal"], fontName="Helvetica", fontSize=9, leading=11)
    head_style = ParagraphStyle("head", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=10, leading=12)

    story = []

    # ---------- Encabezado con logo + títulos ----------
    logo_flow = _make_logo_flowable(logo_url) if logo_url else None
    title_block = [Paragraph(title, styles["Title"])]
    if subtitle:
        title_block.append(Paragraph(subtitle, styles["Italic"]))

    if logo_flow:
        header_tbl = Table(
            [[logo_flow, title_block]],
            colWidths=[40*mm, None]  # 40mm para logo + margen; la derecha se expande
        )
        header_tbl.setStyle(TableStyle([
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ("LEFTPADDING", (0,0), (-1,-1), 0),
            ("RIGHTPADDING", (0,0), (-1,-1), 0),
            ("TOPPADDING", (0,0), (-1,-1), 0),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        story.append(header_tbl)
    else:
        # Fallback sin logo
        story.extend(title_block)

    story.append(Spacer(1, 6))

    # ---------- Sin datos ----------
    if not rows:
        story.append(Paragraph("Sin resultados para los criterios seleccionados.", styles["Italic"]))
        doc.build(story)
        pdf = buf.getvalue(); buf.close()
        return pdf

    # ---------- Tabla de datos ----------
    wrapped_rows = [[Paragraph("" if c is None else str(c), cell_style) for c in r] for r in rows]
    header_pars = [Paragraph(h, head_style) for h in headers]
    data = [header_pars] + wrapped_rows

    if col_widths is None:
        col_widths = [22*mm]*len(headers)

    table = Table(data, colWidths=col_widths, repeatRows=1)
    base_style = [
        ("BACKGROUND", (0,0), (-1,0), colors.white),
        ("LINEBELOW", (0,0), (-1,0), 0.5, colors.HexColor("#6E0707")),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 10),
        ("LEFTPADDING", (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("GRID", (0,0), (-1,-1), 0.25, colors.HexColor("#6E0707")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
            colors.HexColor("#FAB1B1"),  # azul muy pálido
            colors.HexColor("#FCDCDC"),
        ]),
    ]
    if extra_styles:
        base_style.extend(extra_styles)
    table.setStyle(TableStyle(base_style))

    story.append(table)
    doc.build(story)
    pdf = buf.getvalue(); buf.close()
    return pdf