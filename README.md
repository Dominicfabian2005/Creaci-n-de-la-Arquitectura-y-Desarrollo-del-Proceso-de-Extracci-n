# ETL Data Processing Project

Proyecto de procesamiento y carga de datos desarrollado en Python.

##  Descripción
Este proyecto realiza procesos ETL (Extract, Transform, Load) utilizando múltiples archivos CSV para la carga y transformación de datos.

## ⚙️ Funcionalidades
- Extracción de datos desde CSV
- Transformación y limpieza
- Carga de dimensiones
- Organización modular del proyecto
- Manejo de configuraciones mediante JSON

##  Tecnologías utilizadas
- Python
- Pandas
- CSV
- JSON

##  Estructura del proyecto

- `extractors/` → procesos de extracción
- `loaders/` → carga de datos
- `models/` → modelos y estructuras
- `config.json` → configuración
- `main.py` → ejecución principal

##  Cómo ejecutar

```bash
pip install -r requirements.txt
python main.py
