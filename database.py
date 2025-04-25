import pymysql
import os
import datetime

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = int(os.getenv('DB_PORT'))

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
                COALESCE(SUM(id_neto), 0) AS total_neto, 
                COALESCE(SUM(id_descuento), 0) AS total_descuento
            FROM TEARMO01
            WHERE id_fecha BETWEEN %s AND %s
            AND id_contribuyente LIKE %s
        """, (desde_fecha, hasta_fecha, f"%{contribuyente}%"))
    else:
        cursor.execute("""
            SELECT 
                COALESCE(SUM(id_neto), 0) AS total_neto, 
                COALESCE(SUM(id_descuento), 0) AS total_descuento
            FROM TEARMO01
            WHERE id_fecha BETWEEN %s AND %s
        """, (desde_fecha, hasta_fecha))

    resultado = cursor.fetchone()
    conn.close()

    return {
        "total_neto": float(resultado[0]),
        "total_descuento": float(resultado[1])
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

