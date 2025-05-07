FROM apache/airflow:2.7.0

USER root
# Install PostgreSQL development files
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql \
    postgresql-contrib \
    libpq-dev \
    python3-dev \
    gcc \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN mkdir -p /opt/homebrew/bin/ && \
    ln -s /usr/bin/pg_config /opt/homebrew/bin/pg_config || true


ENV PATH="/usr/bin:/opt/homebrew/bin:${PATH}"
ENV PG_CONFIG="/usr/bin/pg_config"

USER airflow

RUN pip install --no-cache-dir psycopg2-binary==2.9.9