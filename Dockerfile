# Utiliser une image Python officielle comme base
FROM python:3.12-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Installer les dépendances système pour OpenCV, pydub (ffmpeg), et autres
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copier uniquement les fichiers de dépendances pour profiter du cache Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code de l'application
COPY . .

# Définir l'environnement par défaut (sera remplacé par -e ou le volume)
ENV ENVIRONMENT=prod

# Exposer le port utilisé par l'application (aligné avec le mappage)
EXPOSE 8002

# Commande pour lancer l'application
CMD ["python", "main.py"]