FROM apache/superset:3.1.1

USER root

# Install PostgreSQL driver and additional dependencies
RUN pip install psycopg2-binary

COPY --chmod=777 superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# Install curl for healthcheck in init script
RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*

USER superset
ENTRYPOINT [ "/usr/bin/run-server.sh" ]
