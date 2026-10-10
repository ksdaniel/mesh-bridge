FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

ENV MESHTASTIC_HOST=192.168.100.102 \
    DB_PATH=/data/messages.db \
    PYTHONUNBUFFERED=1
VOLUME /data
EXPOSE 8000

# Single worker on purpose: one process holds the one TCP connection to the node.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
