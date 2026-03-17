import pandas as pd
import logging
from models.opinion_dto import OpinionDto

logger = logging.getLogger(__name__)

from extractors.iextractor import IExtractor
class CsvExtractor(IExtractor):
    def __init__(self, surveys_path: str, products_path: str):
        self.surveys_path = surveys_path
        self.products_path = products_path

    def extract(self) -> list[OpinionDto]:
        logger.info("Iniciando extracción desde CSV...")
        opiniones = []

        try:
            surveys = pd.read_csv(self.surveys_path)
            products = pd.read_csv(self.products_path)

            df = surveys.merge(products, on="IdProducto", how="left")

            for _, row in df.iterrows():
                opinion = OpinionDto(
                    nombre_producto=row.get("Nombre", "Desconocido"),
                    nombre_cliente=str(row.get("IdCliente", "Desconocido")),
                    fecha=str(row.get("Fecha", "")),
                    nombre_fuente="Encuestas Internas",
                    sentimiento=row.get("Clasificación", "Neutro"),
                    calificacion=int(row.get("PuntajeSatisfacción", 0)),
                    score_sentimiento=None,
                    comentario=row.get("Comentario", ""),
                    id_externo=str(row.get("IdOpinion", ""))
                )
                opiniones.append(opinion)

            logger.info(f"CSV: {len(opiniones)} registros extraídos correctamente.")

        except Exception as e:
            logger.error(f"Error extrayendo CSV: {e}")

        return opiniones