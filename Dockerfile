FROM python:3.12-slim
WORKDIR /app

# Installer les dépendances système pour OpenCV
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
COPY src/environment/prod.env /app/src/environment/prod.env
ENV ENVIRONMENT=prod
EXPOSE 8002
CMD ["python", "main.py"]