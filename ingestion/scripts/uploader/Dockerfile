FROM python:3.8-alpine
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY upload_raw_products.py .

ENTRYPOINT ["sh", "-c", "while true; do python upload_raw_products.py; sleep 300; done"]
