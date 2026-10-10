FROM node:22-alpine AS dashboard
WORKDIR /build/dashboard
COPY dashboard/package*.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build
FROM maven:3.9-eclipse-temurin-17 AS gateway
WORKDIR /build
COPY control-plane/ ./
COPY --from=dashboard /build/control-plane/src/main/resources/static/ ./src/main/resources/static/
RUN mvn -B package
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=gateway /build/target/control-plane-1.0.0.jar ./gateway.jar
COPY app.py engine.py destinations.py start_services.py ./
COPY static ./static
EXPOSE 8083
CMD ["python", "start_services.py"]
