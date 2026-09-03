FROM python:3.11-slim
 
WORKDIR /app
 
RUN pip install --no-cache-dir soda-core-snowflake dagster-pipes
 
COPY . .
 
CMD ["python", "run_with_pipes.py"]
 