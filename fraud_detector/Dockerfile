FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Модель ДЗ-1 и её статистики — версия initial, пока DAG не опубликует свою
COPY models/ models/
COPY artifacts/ artifacts/
COPY src/ src/

ENV PYTHONUNBUFFERED=1
CMD ["python", "src/service.py"]
