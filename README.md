# SQLServerAuditTool

# Aplicación de Auditoría de Base de Datos

Esta es una aplicación de auditoría de base de datos desarrollada en Python utilizando Flask para la interfaz web. La aplicación permite a los usuarios conectarse a una base de datos SQL Server y realizar diversas auditorías, incluyendo la identificación de relaciones y la detección de anomalías de integridad y datos.

## Requisitos

- Python 3.x
- pip (gestor de paquetes de Python)

## Instalación

Sigue estos pasos para configurar y ejecutar la aplicación en tu entorno local:

1. Clona el repositorio o descarga los archivos del proyecto.

2. Crea un entorno virtual:
   ```bash
   python -m venv venv
   
4. Activa el entorno virtual:
    ```bash
   venv\Scripts\activate

6. Instala las dependencias necesarias:
    ```bash
    pip install pyodbc
    pip install Flask

## Ejecución
Una vez instaladas las dependencias, puedes ejecutar la aplicación con el siguiente comando:
   ```bash
    python main.py
