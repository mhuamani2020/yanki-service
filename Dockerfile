# syntax=docker/dockerfile:1
# ============================================================================
# yanki-service — imagen Docker (Parte II: "cada microservicio en un contenedor
# independiente"). Build multi-etapa.
#
# El contexto de build es la RAÍZ del proyecto (ver docker-compose.yml):
#   docker build -f yanki-service/Dockerfile -t yanki-service .
# ============================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /workspace

# 1) Librería compartida de resiliencia (no está publicada en un repositorio Maven)
COPY bank-resilience-commons/pom.xml bank-resilience-commons/pom.xml
COPY bank-resilience-commons/src bank-resilience-commons/src
RUN mvn -q -B -f bank-resilience-commons/pom.xml -DskipTests install

# 2) Microservicio
COPY yanki-service/pom.xml yanki-service/pom.xml
COPY yanki-service/src yanki-service/src
RUN mvn -q -B -f yanki-service/pom.xml -DskipTests package

# ---------- Etapa de ejecución ----------
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /workspace/yanki-service/target/*.jar app.jar
EXPOSE 8085
HEALTHCHECK --interval=10s --timeout=5s --start-period=45s --retries=12 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8085'
ENTRYPOINT ["java","-jar","/app/app.jar"]
