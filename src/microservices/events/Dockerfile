# Используем легкий образ Python 3.11
FROM python:3.11-slim

# Рабочая директория
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код приложения
COPY . /app



# Запуск FastAPI через Uvicorn на 0.0.0.0:8082
ENTRYPOINT ["python", "-u", "main.py"]