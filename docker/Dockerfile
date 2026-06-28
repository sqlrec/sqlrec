# Stage 1: Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Build the project (skip tests to speed up build)
RUN --mount=type=cache,target=/root/.m2,rw \
    --mount=type=cache,target=/root/.npm,rw \
    --mount=type=cache,target=/app/sqlrec-ui/.node,rw \
    --mount=type=cache,target=/app/sqlrec-ui/src/main/frontend/node_modules,rw \
    mvn clean package -DskipTests

# Stage 2: Runtime stage
FROM eclipse-temurin:25-jre

# Set working directory
WORKDIR /app

ENV PATH="$PATH:/app"

# Copy built artifacts and script from build stage
COPY --from=builder /app/sqlrec-frontend/target/sqlrec-frontend-*.jar ./
COPY --from=builder /app/bin/server.sh ./
COPY --from=builder /app/bin/cli.sh ./

# Set script as executable
RUN chmod +x server.sh && chmod +x cli.sh

# Use script as entry point
ENTRYPOINT ["sh", "server.sh"]