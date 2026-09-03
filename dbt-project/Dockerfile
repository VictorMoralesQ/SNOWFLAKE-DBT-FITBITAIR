FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-snowflake dagster-pipes

COPY . .

CMD ["python", "run_with_pipes.py"]