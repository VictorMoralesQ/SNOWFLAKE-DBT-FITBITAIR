FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN mkdir -p $DAGSTER_HOME

# Dependencias del sistema + docker CLI (útil para debug del socket montado)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# --- Dependencias Python ---
# Nota: como el contexto de build es esta misma carpeta, el
# requirements.txt debe existir aquí dentro (no se puede copiar
# el de la raíz del repo con este build context). Si prefieres
# mantener uno solo, cámbialo por un symlink o duplícalo al
# empaquetar, o cambia el context del compose a la raíz.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt


COPY . .

EXPOSE 3000

CMD ["dagster", "dev", "-f", "definitions.py", "-h", "0.0.0.0", "-p", "3000"]