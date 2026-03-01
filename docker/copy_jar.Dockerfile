FROM eclipse-temurin:21-jre-alpine

# Set working directory
WORKDIR /app

ENV PATH="$PATH:/app"

# Copy built artifacts and script
COPY ./sqlrec-frontend/target/sqlrec-frontend-*.jar ./
COPY ./bin/sqlrec ./

# Set script as executable
RUN chmod +x sqlrec

# Use script as entry point
ENTRYPOINT ["sh", "sqlrec"]