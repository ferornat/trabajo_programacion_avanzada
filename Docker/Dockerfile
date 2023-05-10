FROM python:3.9-slim-bullseye

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["uvicorn", "api_tp:app", "--host", "0.0.0.0", "--port", "8080"] 	
