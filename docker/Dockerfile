# Stage 1: Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Download all dependencies
RUN mvn dependency:go-offline -B

# Build the project (skip tests to speed up build)
RUN mvn clean package -DskipTests

# Stage 2: Runtime stage
FROM eclipse-temurin:21-jre-alpine

# Set working directory
WORKDIR /app

ENV PATH="$PATH:/app"

# Copy built artifacts and script from build stage
COPY --from=builder /app/sqlrec-frontend/target/sqlrec-frontend-*.jar sqlrec-frontend.jar
COPY --from=builder /app/bin/sqlrec sqlrec

# Set script as executable
RUN chmod +x sqlrec

# Use script as entry point
ENTRYPOINT ["sh", "sqlrec"]