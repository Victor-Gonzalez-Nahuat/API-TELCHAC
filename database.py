import pymysql
import os
import datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from datetime import datetime

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = int(os.getenv('DB_PORT'))

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
            direccion
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
        }
        for row in resultados
    ]
    return cedulas


def obtenerCedulasConIntervaloYContribuyente(desde_fecha, hasta_fecha, contribuyente):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT LEFT(codigo, 6), motivo, fecham, contribuyente, direccion 
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

def build_pdf(title: str, subtitle: str, headers: list[str], rows: list[list[str]]) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(title, styles["Title"]))
    if subtitle:
        story.append(Paragraph(subtitle, styles["Normal"]))
    story.append(Spacer(1, 12))

    # Si no hay filas, ponemos un mensaje claro
    if not rows:
        story.append(Paragraph("Sin resultados para los criterios seleccionados.", styles["Italic"]))
        doc.build(story)
        pdf = buf.getvalue()
        buf.close()
        return pdf

    data = [headers] + rows
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
    ]))
    story.append(table)
    doc.build(story)

    pdf = buf.getvalue()
    buf.close()
    return pdf

def _attachment_headers(filename: str) -> dict:
    return {"Content-Disposition": f'attachment; filename="{filename}"'}

