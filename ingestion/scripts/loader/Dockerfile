FROM python:3.8-alpine
WORKDIR /app
RUN apk add --no-cache gcc musl-dev postgresql-dev

COPY ingestion/scripts/loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/scripts/loader/load_raw_to_pg.py .

ENTRYPOINT ["python", "load_raw_to_pg.py"]
