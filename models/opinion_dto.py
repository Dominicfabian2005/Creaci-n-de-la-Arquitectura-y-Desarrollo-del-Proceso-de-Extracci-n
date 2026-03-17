from dataclasses import dataclass
from typing import Optional

@dataclass
class OpinionDto:
    nombre_producto: str
    nombre_cliente: str
    fecha: str
    nombre_fuente: str
    sentimiento: str
    calificacion: Optional[int]
    score_sentimiento: Optional[float]
    comentario: Optional[str]
    id_externo: Optional[str]