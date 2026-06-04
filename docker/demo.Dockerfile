ARG SQLREC_VERSION=latest

# Stage 1: Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/root/.m2,rw \
    mvn clean package -DskipTests -pl sqlrec-demo -am

# Stage 2: Runtime stage
FROM sqlrec/sqlrec:${SQLREC_VERSION}

WORKDIR /app

COPY --from=builder /app/sqlrec-demo/target/sqlrec-demo-*.jar ./
COPY --from=builder /app/sqlrec-demo/src/main/sql ./sql

ENV SQL_SCHEMA_DIR=/app/sql
