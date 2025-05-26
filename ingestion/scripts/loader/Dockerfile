FROM python:3.8-alpine
WORKDIR /app
RUN apk add --no-cache gcc musl-dev postgresql-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_raw_to_pg.py .

ENTRYPOINT ["python", "load_raw_to_pg.py"]
