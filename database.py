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



LOGO_URL = "https://i.ibb.co/SDz9CZXS/Imagen-de-Whats-App-2025-04-22-a-las-15-46-24-f6a2c21e.jpg"


def expandir_rango_fechas(desde, hasta):
    """Ajusta el rango para que hasta incluya todo el día."""
    desde_dt = datetime.datetime.strptime(desde, '%Y-%m-%d')
    hasta_dt = datetime.datetime.strptime(hasta, '%Y-%m-%d') + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    return desde_dt, hasta_dt


def get_connection():
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
    DB_PORT = int(os.getenv('DB_PORT'))
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
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente, id_dispo6
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
            "porcentaje_descuento": row[6]
        } 
        for row in resultados
    ]
    return recibos

def obtenerRecibosConIntervalo(desde_fecha, hasta_fecha):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente, id_dispo6, id_formapago
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
            "porcentaje_descuento": row[6] if row[6] and row[6] != "000000" else 0,
            "forma_pago": row[7]
        } 
        for row in resultados
    ]
    return recibos

def obtenerRecibosHoy():
    conn = get_connection()
    cursor = conn.cursor()

    fecha_hoy = datetime.datetime.today().strftime('%y%m%d')

    cursor.execute("""
        SELECT id_recibo, id_fecha, id_neto, id_descuento, id_concepto1, id_contribuyente, id_dispo6, id_formapago
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
            "porcentaje_descuento": row[6] if row[6] else 0,
            "forma_pago": row[7]
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
            COALESCE(SUM(m.id_descuento), 0) AS total_descuento,
            COUNT(*) AS cantidad_recibos
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
            "cuenta": row[0], 
            "total_neto": float(row[1]),
            "total_descuento": float(row[2]),
            "cantidad_recibos": int(row[3])
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
    logo_url: str | None = LOGO_URL,
) -> bytes:
    buf = BytesIO()
    pagesize = landscape(A4) if landscape_mode else A4
    # Definición de colores institucionales
    GOB_GUINDA = colors.HexColor("#691C32")
    GOB_DORADO = colors.HexColor("#BC955C")
    GOB_GRIS_F = colors.HexColor("#F2F2F2")

    doc = SimpleDocTemplate(
        buf,
        pagesize=pagesize,
        leftMargin=12*mm, rightMargin=12*mm,
        topMargin=10*mm, bottomMargin=10*mm,
        title=title
    )
    
    styles = getSampleStyleSheet()
    
    # Estilos de texto personalizados
    title_style = ParagraphStyle(
        "GovTitle", parent=styles["Title"],
        fontName="Helvetica-Bold", fontSize=16,
        textColor=GOB_GUINDA, alignment=0 # Alineado a la izquierda
    )
    sub_style = ParagraphStyle(
        "GovSub", parent=styles["Normal"],
        fontName="Helvetica", fontSize=10,
        textColor=colors.gray, leading=12
    )
    cell_style = ParagraphStyle(
        "cell", parent=styles["Normal"], 
        fontName="Helvetica", fontSize=8, leading=10
    )

    story = []

    # ---------- ENCABEZADO INSTITUCIONAL ----------
    logo_flow = _make_logo_flowable(logo_url, max_w=40*mm, max_h=25*mm) if logo_url else None
    
    # Textos del encabezado
    header_info = [
        Paragraph(title.upper(), title_style),
        Paragraph(subtitle, sub_style),
        Paragraph(f"Fecha de impresión: {datetime.now().strftime('%d/%m/%Y %H:%M')}", sub_style)
    ]

    # Tabla de encabezado para alinear logo y textos
    header_data = [[logo_flow, header_info]]
    header_tbl = Table(header_data, colWidths=[45*mm, None])
    header_tbl.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 10),
    ]))
    story.append(header_tbl)

    # --- BARRA DECORATIVA (ESTILO GOBIERNO) ---
    # Una línea guinda gruesa con una dorada delgada abajo
    linea_guinda = Table([[""]], colWidths=[doc.width])
    linea_guinda.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), GOB_GUINDA),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(linea_guinda)
    story.append(Spacer(1, 2)) # Espacio mínimo entre barras
    
    linea_dorada = Table([[""]], colWidths=[doc.width])
    linea_dorada.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), GOB_DORADO),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
    ]))
    story.append(linea_dorada)
    story.append(Spacer(1, 8))

    # ---------- TABLA DE DATOS ----------
    if not rows:
        story.append(Paragraph("No se encontraron registros.", styles["Italic"]))
    else:
        wrapped_rows = [[Paragraph(str(c) if c else "", cell_style) for c in r] for r in rows]
        # Headers con fondo guinda y texto blanco
        header_pars = [Paragraph(f"<b>{h}</b>", ParagraphStyle("h", textColor=colors.white, fontSize=9, alignment=1)) for h in headers]
        data = [header_pars] + wrapped_rows

        table = Table(data, colWidths=col_widths, repeatRows=1)
        
        base_style = [
            ("BACKGROUND", (0,0), (-1,0), GOB_GUINDA), # Encabezado guinda
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ("GRID", (0,0), (-1,-1), 0.1, colors.gray), # Cuadrícula muy fina
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, GOB_GRIS_F]), # Alternancia de gris muy tenue
        ]
        
        if extra_styles:
            base_style.extend(extra_styles)
            
        table.setStyle(TableStyle(base_style))
        story.append(table)

    doc.build(story)
    return buf.getvalue()