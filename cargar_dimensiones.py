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
logger.info(" Conexión exitosa a sistema_opiniones")
cursor = conn.cursor()

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
    # Columnas: id_producto(IDENTITY), nombre_producto, categoria, marca
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_producto...")
    for _, row in products.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_producto WHERE nombre_producto = ?)
            INSERT INTO dim_producto (nombre_producto, categoria, marca)
            VALUES (?, ?, ?)
        """,
        str(row["Nombre"]),
        str(row["Nombre"]),
        str(row.get("Categoría", "Sin Categoría")),
        "Sin Marca")
    conn.commit()
    logger.info(" dim_producto cargada")

    # ─────────────────────────────────────────────
    # DIM_CLIENTE
    # Columnas: id_cliente(IDENTITY), nombre, pais, edad, tipo_cliente
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
            IF NOT EXISTS (SELECT 1 FROM dim_cliente WHERE nombre = ?)
            INSERT INTO dim_cliente (nombre, pais, edad, tipo_cliente)
            VALUES (?, ?, ?, ?)
        """,
        id_c,
        id_c,
        "Desconocido",
        "0",
        tipo)
    conn.commit()
    logger.info(" dim_cliente cargada")

    # ─────────────────────────────────────────────
    # DIM_FECHA
    # Columnas: id_fecha(IDENTITY), fecha, anio, mes, trimestre
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_fecha...")
    fechas = set()
    fechas.update(surveys["Fecha"].dropna().astype(str).str[:10].tolist())
    fechas.update(reviews["Fecha"].dropna().astype(str).str[:10].tolist())
    fechas.update(social["Fecha"].dropna().astype(str).str[:10].tolist())

    for fecha_str in sorted(fechas):
        try:
            f = pd.to_datetime(fecha_str[:10])
            trimestre = (f.month - 1) // 3 + 1
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM dim_fecha WHERE fecha = ?)
                INSERT INTO dim_fecha (fecha, anio, mes, trimestre)
                VALUES (?, ?, ?, ?)
            """,
            f.date(),
            f.date(),
            f.year,
            f.month,
            trimestre)
        except Exception as e:
            logger.warning(f"Fecha inválida {fecha_str}: {e}")
    conn.commit()
    logger.info(" dim_fecha cargada")

    # ─────────────────────────────────────────────
    # DIM_FUENTE
    # Columnas: id_fuente(IDENTITY), nombre_fuente
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_fuente...")
    fuentes = [
        "Encuestas Internas",
        "Reseñas Web",
        "Instagram",
        "Twitter",
        "Redes Sociales",
        "API Externa"
    ]
    for nombre in fuentes:
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_fuente WHERE nombre_fuente = ?)
            INSERT INTO dim_fuente (nombre_fuente)
            VALUES (?)
        """, nombre, nombre)
    conn.commit()
    logger.info(" dim_fuente cargada")

    # ─────────────────────────────────────────────
    # DIM_SENTIMIENTO
    # Columnas: id_sentimiento(IDENTITY), tipo
    # ─────────────────────────────────────────────
    logger.info("Cargando dim_sentimiento...")
    sentimientos = {"Positiva", "Negativa", "Neutra", "Neutro", "Desconocido"}
    sentimientos.update(surveys["Clasificación"].dropna().unique().tolist())

    for tipo in sorted(sentimientos):
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dim_sentimiento WHERE tipo = ?)
            INSERT INTO dim_sentimiento (tipo)
            VALUES (?)
        """, tipo, tipo)
    conn.commit()
    logger.info(" dim_sentimiento cargada")

    # ─────────────────────────────────────────────
    # MAPAS DE LOOKUP PARA FACT_OPINION
    # ─────────────────────────────────────────────
    cursor.execute("SELECT nombre_producto, id_producto FROM dim_producto")
    map_producto = {str(r[0]): r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT nombre, id_cliente FROM dim_cliente")
    map_cliente = {str(r[0]): r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT fecha, id_fecha FROM dim_fecha")
    map_fecha = {str(r[0])[:10]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT nombre_fuente, id_fuente FROM dim_fuente")
    map_fuente = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT tipo, id_sentimiento FROM dim_sentimiento")
    map_sentimiento = {r[0]: r[1] for r in cursor.fetchall()}

    def safe_float(val):
        try:
            return float(val) if pd.notna(val) else None
        except:
            return None

    # ─────────────────────────────────────────────
    # FACT_OPINION
    # Columnas: id_opinion(IDENTITY), id_producto, id_cliente,
    #           id_fecha, id_fuente, id_sentimiento, calificacion, comentario
    # ─────────────────────────────────────────────
    logger.info("Cargando fact_opinion...")
    insertados = 0

    sql_fact = """
        INSERT INTO fact_opinion
            (id_producto, id_cliente, id_fecha, id_fuente, id_sentimiento, calificacion, comentario)
        VALUES (?,?,?,?,?,?,?)
    """

    # --- ENCUESTAS (surveys_part1.csv) ---
    for _, row in surveys.iterrows():
        fecha = str(row.get("Fecha", ""))[:10]
        nombre_prod = "Producto_" + str(row.get("IdProducto", ""))
        cursor.execute(sql_fact, (
            map_producto.get(nombre_prod),
            map_cliente.get(str(row.get("IdCliente", ""))),
            map_fecha.get(fecha),
            map_fuente.get("Encuestas Internas"),
            map_sentimiento.get(str(row.get("Clasificación", "Neutro")),
                                map_sentimiento.get("Neutro")),
            safe_float(row.get("PuntajeSatisfacción")),
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    # --- RESEÑAS WEB (web_reviews.csv) ---
    for _, row in reviews.iterrows():
        fecha = str(row.get("Fecha", ""))[:10]
        id_prod_raw = str(row.get("IdProducto", "")).replace("P0", "").replace("P", "")
        nombre_prod = "Producto_" + id_prod_raw
        cursor.execute(sql_fact, (
            map_producto.get(nombre_prod),
            map_cliente.get(str(row.get("IdCliente", ""))),
            map_fecha.get(fecha),
            map_fuente.get("Reseñas Web"),
            map_sentimiento.get("Neutro"),
            safe_float(row.get("Rating")),
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    # --- COMENTARIOS SOCIALES (social_comments.csv) ---
    for _, row in social.iterrows():
        fecha = str(row.get("Fecha", ""))[:10]
        id_prod_raw = str(row.get("IdProducto", "")).replace("P0", "").replace("P", "")
        nombre_prod = "Producto_" + id_prod_raw
        fuente_src = str(row.get("Fuente", "Redes Sociales"))
        id_c = str(row.get("IdCliente", "Anónimo"))
        if id_c == "nan":
            id_c = "Anónimo"
        cursor.execute(sql_fact, (
            map_producto.get(nombre_prod),
            map_cliente.get(id_c),
            map_fecha.get(fecha),
            map_fuente.get(fuente_src, map_fuente.get("Redes Sociales")),
            map_sentimiento.get("Neutro"),
            None,
            str(row.get("Comentario", ""))
        ))
        insertados += 1

    conn.commit()
    logger.info(f" fact_opinion cargada: {insertados} registros")

    # ─────────────────────────────────────────────
    # RESUMEN FINAL
    # ─────────────────────────────────────────────
    logger.info("=" * 50)
    logger.info("RESUMEN DEL DATAWAREHOUSE")
    logger.info("=" * 50)
    for tabla in ["dim_producto", "dim_cliente", "dim_fecha", "dim_fuente", "dim_sentimiento", "fact_opinion"]:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        n = cursor.fetchone()[0]
        logger.info(f"  {tabla:<25} {n:>6} registros")
    logger.info("=" * 50)

except Exception as e:
    logger.error(f" ERROR: {e}")
    conn.rollback()
    raise
finally:
    conn.close()