import pyodbc
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=sistema_opiniones;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;",
    timeout=10
)
print("Conexión exitosa")
cursor = conn.cursor()
logger.info("Conexión exitosa a sistema_opiniones")

try:
    # ─────────────────────────────────────────────
    # LEER CSVs
    # ─────────────────────────────────────────────
    surveys  = pd.read_csv("surveys_part1.csv")
    reviews  = pd.read_csv("web_reviews.csv")
    social   = pd.read_csv("social_comments.csv")
    products = pd.read_csv("products.csv")

    # ─────────────────────────────────────────────
    # DIM_PRODUCTO
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_producto...")
    for _, row in products.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_producto WHERE id_producto = ?)
            INSERT INTO dim_producto (id_producto, nombre_producto, categoria, marca)
            VALUES (?, ?, ?, ?)
        """, str(row["IdProducto"]), str(row["IdProducto"]),
            str(row["Nombre"]), str(row.get("Categoría", "Sin Categoría")), "Sin Marca")
    conn.commit()
    logger.info("✅ dim_producto cargada")

    # ─────────────────────────────────────────────
    # DIM_CLIENTE
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_cliente...")
    clientes = set()
    for id_c in surveys["IdCliente"].dropna().unique():
        clientes.add((str(id_c), "Encuesta"))
    for id_c in reviews["IdCliente"].dropna().unique():
        clientes.add((str(id_c), "Web"))
    for id_c in social["IdCliente"].dropna().unique():
        clientes.add((str(id_c), "Social"))

    for id_c, tipo in clientes:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_cliente WHERE id_cliente = ?)
            INSERT INTO dim_cliente (id_cliente, nombre, pais, edad, tipo_cliente)
            VALUES (?, ?, ?, ?, ?)
        """, id_c, id_c, "Desconocido", "Desconocido", None, tipo)
    conn.commit()
    logger.info("✅ dim_cliente cargada")

    # ─────────────────────────────────────────────
    # DIM_FECHA
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_fecha...")
    fechas = set()
    fechas.update(surveys["Fecha"].dropna().astype(str).str[:10].tolist())
    fechas.update(reviews["Fecha"].dropna().astype(str).str[:10].tolist())
    fechas.update(social["Fecha"].dropna().astype(str).str[:10].tolist())

    for fecha_str in sorted(fechas):
        try:
            f = pd.to_datetime(fecha_str[:10])
            id_fecha = int(f.strftime("%Y%m%d"))
            trimestre = (f.month - 1) // 3 + 1
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM dim_fecha WHERE id_fecha = ?)
                INSERT INTO dim_fecha (id_fecha, fecha, anio, mes, trimestre)
                VALUES (?, ?, ?, ?, ?)
            """, id_fecha, id_fecha, f.date(), f.year, f.month, trimestre)
        except Exception as e:
            logger.warning(f"Fecha inválida {fecha_str}: {e}")
    conn.commit()
    logger.info("✅ dim_fecha cargada")

    # ─────────────────────────────────────────────
    # DIM_FUENTE
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_fuente...")
    fuentes = [("Encuestas Internas", "CSV"), ("Reseñas Web", "Web"),
               ("Instagram", "Social"), ("Twitter", "Social"),
               ("Redes Sociales", "Social"), ("API Externa", "API")]

    for nombre, tipo in fuentes:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_fuente WHERE nombre_fuente = ?)
            INSERT INTO dim_fuente (nombre_fuente, tipo_fuente)
            VALUES (?, ?)
        """, nombre, nombre, tipo)
    conn.commit()
    logger.info("✅ dim_fuente cargada")

    # ─────────────────────────────────────────────
    # DIM_SENTIMIENTO
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_sentimiento...")
    sentimientos_data = [
        ("Positiva", "Positivo"), ("Negativa", "Negativo"),
        ("Neutra", "Neutro"), ("Neutro", "Neutro"), ("Desconocido", "Neutro")
    ]
    sentimientos_csv = surveys["Clasificación"].dropna().unique().tolist()
    for s in sentimientos_csv:
        pol = "Positivo" if "pos" in s.lower() else "Negativo" if "neg" in s.lower() else "Neutro"
        sentimientos_data.append((s, pol))

    for clasif, polaridad in sentimientos_data:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_sentimiento WHERE clasificacion = ?)
            INSERT INTO dim_sentimiento (clasificacion, polaridad)
            VALUES (?, ?)
        """, clasif, clasif, polaridad)
    conn.commit()
    logger.info("✅ dim_sentimiento cargada")

    # ─────────────────────────────────────────────
    # FACT_OPINION
    # ─────────────────────────────────────────────
    logger.info("Cargando fact_opinion...")

    # Mapas de lookup
    cursor.execute("SELECT id_producto, id_producto FROM dim_producto")
    map_producto = {str(r[0]): r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT id_cliente, id_cliente FROM dim_cliente")
    map_cliente = {str(r[0]): r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT id_fecha, id_fecha FROM dim_fecha")
    map_fecha = {str(r[0]): r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT nombre_fuente, id_fuente FROM dim_fuente")
    map_fuente = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT clasificacion, id_sentimiento FROM dim_sentimiento")
    map_sentimiento = {r[0]: r[1] for r in cursor.fetchall()}

    def get_id_fecha(fecha_str):
        try:
            return int(pd.to_datetime(str(fecha_str)[:10]).strftime("%Y%m%d"))
        except:
            return None

    insertados = 0
    sql_fact = """
        INSERT INTO fact_opinion
            (id_producto, id_cliente, id_fecha, id_fuente, id_sentimiento, calificacion, comentario)
        VALUES (?,?,?,?,?,?,?)
    """

    # Encuestas
    for _, row in surveys.iterrows():
        cursor.execute(sql_fact, (
            map_producto.get(str(row["IdProducto"])),
            map_cliente.get(str(row["IdCliente"])),
            get_id_fecha(row["Fecha"]),
            map_fuente.get("Encuestas Internas"),
            map_sentimiento.get(str(row.get("Clasificación", "Neutro")), map_sentimiento.get("Neutro")),
            float(row["PuntajeSatisfacción"]) if pd.notna(row.get("PuntajeSatisfacción")) else None,
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    # Reseñas Web
    for _, row in reviews.iterrows():
        cursor.execute(sql_fact, (
            map_producto.get(str(row["IdProducto"])),
            map_cliente.get(str(row["IdCliente"])),
            get_id_fecha(row["Fecha"]),
            map_fuente.get("Reseñas Web"),
            map_sentimiento.get("Neutro"),
            float(row["Rating"]) if pd.notna(row.get("Rating")) else None,
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    # Comentarios Sociales
    for _, row in social.iterrows():
        fuente_src = str(row.get("Fuente", "Redes Sociales"))
        cursor.execute(sql_fact, (
            map_producto.get(str(row["IdProducto"])),
            map_cliente.get(str(row.get("IdCliente", "Anónimo"))),
            get_id_fecha(row["Fecha"]),
            map_fuente.get(fuente_src, map_fuente.get("Redes Sociales")),
            map_sentimiento.get("Neutro"),
            None,
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    conn.commit()
    logger.info(f"✅ fact_opinion cargada: {insertados} registros")

    # ─────────────────────────────────────────────
    # RESUMEN
    # ─────────────────────────────────────────────
    logger.info("=" * 50)
    for tabla in ["dim_producto", "dim_cliente", "dim_fecha", "dim_fuente", "dim_sentimiento", "fact_opinion"]:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        n = cursor.fetchone()[0]
        logger.info(f"  {tabla:<25} {n:>6} registros")
    logger.info("=" * 50)

except Exception as e:
    logger.error(f"❌ ERROR: {e}")
    conn.rollback()
    raise
finally:
    conn.close()