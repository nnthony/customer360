# Imagen mínima para ejecutar las pruebas (y el pipeline) sin instalar Python.
# Misma versión de Python que el entorno de desarrollo y el CI.
FROM python:3.10-slim

WORKDIR /app

# No generar .pyc y mostrar la salida sin buffer (logs en tiempo real).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Primero SOLO requirements.txt: si el código cambia pero las dependencias no,
# Docker reutiliza esta capa y no vuelve a instalar duckdb.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Después el resto del proyecto (lo que excluye .dockerignore no se copia).
COPY . .

# Por defecto: ejecutar las pruebas. Se puede sobrescribir en `docker run`.
CMD ["python", "-m", "pytest", "-ra"]
