# stream-processor/Dockerfile
FROM openjdk:11-jre-slim
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY processor.py .
CMD ["python3", "processor.py"]