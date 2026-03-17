import requests
import logging
from models.opinion_dto import OpinionDto
from extractors.iextractor import IExtractor

logger = logging.getLogger(__name__)

class ApiExtractor(IExtractor):
    def __init__(self, api_url: str):
        self.api_url = api_url

    def extract(self) -> list[OpinionDto]:
        logger.info("Iniciando extracción desde API REST...")
        opiniones = []

        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            datos = response.json()

            for item in datos:
                opinion = OpinionDto(
                    nombre_producto="Producto General",
                    nombre_cliente=item.get("email", "Desconocido"),
                    fecha="2024-01-01",
                    nombre_fuente="Redes Sociales",
                    sentimiento="Neutro",
                    calificacion=None,
                    score_sentimiento=None,
                    comentario=item.get("body", ""),
                    id_externo=str(item.get("id", ""))
                )
                opiniones.append(opinion)

            logger.info(f"API REST: {len(opiniones)} registros extraídos correctamente.")

        except Exception as e:
            logger.error(f"Error extrayendo API: {e}")

        return opiniones