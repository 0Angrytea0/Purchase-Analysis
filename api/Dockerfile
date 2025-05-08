FROM python:3.10-slim
WORKDIR /app

# Установим Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код FastAPI
COPY src/ ./src/

EXPOSE 8000

# Запуск Uvicorn
CMD ["uvicorn","src.app:app","--host","0.0.0.0","--port","8000","--reload"]
