FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
COPY src/environment/prod.env /app/src/environment/prod.env
ENV ENVIRONMENT=prod
EXPOSE 8002
CMD ["python", "main.py"]