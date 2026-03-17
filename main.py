import logging
import json
import csv
import time
from extractors.csv_extractor import CsvExtractor
from extractors.database_extractor import DatabaseExtractor
from extractors.api_extractor import ApiExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def cargar_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def guardar_staging(opiniones, archivo):
    with open(archivo, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "nombre_producto", "nombre_cliente", "fecha",
            "nombre_fuente", "sentimiento", "calificacion",
            "score_sentimiento", "comentario", "id_externo"
        ])
        for o in opiniones:
            writer.writerow([
                o.nombre_producto, o.nombre_cliente, o.fecha,
                o.nombre_fuente, o.sentimiento, o.calificacion,
                o.score_sentimiento, o.comentario, o.id_externo
            ])
    logger.info(f"Staging guardado en: {archivo}")

def main():
    logger.info("=== Iniciando proceso de Extracción ETL ===")
    config = cargar_config()

   
    inicio = time.time()
    csv_extractor = CsvExtractor(
        surveys_path=config["sources"]["csv_surveys"],
        products_path=config["sources"]["csv_products"]
    )
    opiniones_csv = csv_extractor.extract()
    guardar_staging(opiniones_csv, "staging_encuestas.csv")
    logger.info(f"Tiempo CSV: {time.time() - inicio:.2f} segundos")

    
    inicio = time.time()
    db_extractor = DatabaseExtractor(
        server=config["database"]["server"],
        database=config["database"]["name"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        reviews_path=config["sources"]["csv_web_reviews"],
        products_path=config["sources"]["csv_products"]
    )
    opiniones_db = db_extractor.extract()
    guardar_staging(opiniones_db, "staging_resenas.csv")
    logger.info(f"Tiempo Base de Datos: {time.time() - inicio:.2f} segundos")

   
    inicio = time.time()
    api_extractor = ApiExtractor(
        api_url=config["api"]["url"]
    )
    opiniones_api = api_extractor.extract()
    guardar_staging(opiniones_api, "staging_redes.csv")
    logger.info(f"Tiempo API REST: {time.time() - inicio:.2f} segundos")


    total = len(opiniones_csv) + len(opiniones_db) + len(opiniones_api)
    logger.info("=== Extracción completada ===")
    logger.info(f"Encuestas CSV:   {len(opiniones_csv)} registros")
    logger.info(f"Reseñas Web:     {len(opiniones_db)} registros")
    logger.info(f"Redes Sociales:  {len(opiniones_api)} registros")
    logger.info(f"TOTAL extraído:  {total} registros")

if __name__ == "__main__":
    main()