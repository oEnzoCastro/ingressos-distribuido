FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proto/ proto/
COPY src/ src/
COPY gerar_proto.py .

# Gera o código gRPC a partir do .proto
RUN python gerar_proto.py

CMD ["python", "src/servidor.py"]
