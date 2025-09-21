FROM python:3.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y build-essential
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--lifespan", "on", "--log-level", "debug"]
