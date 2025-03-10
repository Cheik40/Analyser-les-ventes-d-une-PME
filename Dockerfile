# Utilisation de l'image Python officielle comme base
FROM python:3.9-slim

# Définir un répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt /app/

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Créer le répertoire de la base de données dans le conteneur
RUN mkdir -p /app/data/db

# Copier le fichier Python dans le conteneur
COPY main.py /app/

# Exposer le port sur lequel ton application écoutera (si nécessaire)
# EXPOSE 5000

# Définir la commande d'exécution du conteneur
CMD ["python", "main.py"]
