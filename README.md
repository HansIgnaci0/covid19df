# Proyecto COVID-19 - Análisis con Kedro

Este proyecto realiza un análisis exploratorio y limpieza de datos COVID-19 usando **Kedro**.  
Incluye varios datasets, limpieza de valores nulos y duplicados, y generación de gráficos.

---

## Contenido del proyecto

- `data/01_raw/` - Datasets originales (CSV)
- `data/02_reporting/` - Reportes de nulos y duplicados
- `data/03_intermediate/` - Datasets limpios (CSV)
- `data/04_models/` - Gráficos generados (PNG)
- `src/nodes.py` - Nodos de Kedro (limpieza, EDA, gráficos)
- `src/pipeline.py` - Pipeline Kedro
- `requirements.txt` - Dependencias del proyecto

---

## Requisitos

- Python >= 3.9
- [Kedro](https://kedro.readthedocs.io/en/stable/)
- Pandas
- Matplotlib

> Todas las dependencias se pueden instalar con `requirements.txt`.

---

## Instalación

1. Clonar el repositorio:

``bash
git clone <url-del-proyecto>
cd <nombre-del-proyecto>

2. Crear y activar un entorno virtual (recomendado):
   python -m venv venv_kedro
# Windows
.\venv_kedro\Scripts\activate
# Linux / macOS
source venv_kedro/bin/activate

3. Instalar dependencias
   pip install -r requirements.txt
   
## Ejecución

1. Inicializar el proyecto Kedro (si no está inicializado):
   
 kedro install

3. Ejecutar pipelines:
   
   kedro run

5. (Opcional) Visualizar el pipeline:
   
   kedro viz
 


