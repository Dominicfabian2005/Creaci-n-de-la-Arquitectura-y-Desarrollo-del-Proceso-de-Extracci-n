import logging
import pandas as pd
from models.opinion_dto import OpinionDto

logger = logging.getLogger(__name__)

class DatabaseExtractor:
    def __init__(self, server: str, database: str, user: str, password: str, reviews_path: str, products_path: str):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.reviews_path = reviews_path
        self.products_path = products_path

    def extract(self) -> list[OpinionDto]:
        logger.info("Iniciando extracción desde Base de Datos...")
        opiniones = []

        try:
            reviews = pd.read_csv(self.reviews_path)
            products = pd.read_csv(self.products_path)

            reviews["IdProducto"] = reviews["IdProducto"].astype(str)
            products["IdProducto"] = products["IdProducto"].astype(str)

            df = reviews.merge(products, on="IdProducto", how="left")

            for _, row in df.iterrows():
                opinion = OpinionDto(
                    nombre_producto=row.get("Nombre", "Desconocido"),
                    nombre_cliente=str(row.get("IdCliente", "Desconocido")),
                    fecha=str(row.get("Fecha", "")),
                    nombre_fuente="Reseñas Web",
                    sentimiento="Neutro",
                    calificacion=int(row.get("Rating", 0)),
                    score_sentimiento=None,
                    comentario=row.get("Comentario", ""),
                    id_externo=str(row.get("IdReview", ""))
                )
                opiniones.append(opinion)

            logger.info(f"Base de Datos: {len(opiniones)} registros extraídos correctamente.")

        except Exception as e:
            logger.error(f"Error extrayendo Base de Datos: {e}")

        return opiniones