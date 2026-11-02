# Image unique pour le collecteur, le Reference Loader, le producteur et l'injecteur.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Utilisateur non root
RUN useradd --uid 10001 --create-home app
USER app

# La commande est choisie par le service (collector.py, reference_loader.py, producer.py…).
CMD ["python", "producer.py"]
