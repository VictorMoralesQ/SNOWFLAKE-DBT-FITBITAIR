import os
from datetime import datetime, timedelta, timezone

import requests
import snowflake.connector
from dagster_pipes import open_dagster_pipes
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

HEALTH_API_BASE = "https://health.googleapis.com/v4/users/me/dataTypes"
TOKEN_URI = "https://oauth2.googleapis.com/token"

GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
GOOGLE_REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]

SNOWFLAKE_CONFIG = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
}

DATA_TYPES = {
    "steps": "activity_and_fitness",
    "heart-rate": "health_metrics_and_measurements",
    "sleep": "sleep",
}


def get_credentials() -> Credentials:
    """Construye credenciales OAuth2 a partir del refresh token guardado
    y refresca el access token si hace falta (google-auth lo gestiona solo)."""
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri=TOKEN_URI,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
    )
    creds.refresh(Request())
    return creds


def fetch_data_type(session: requests.Session, data_type: str, start: str, end: str) -> dict:
    """Usa la operación 'reconcile', que devuelve los datos ya normalizados
    entre dispositivos (recomendado sobre 'list' según la doc de la API)."""
    url = f"{HEALTH_API_BASE}/{data_type}/dataPoints:reconcile"
    params = {"startTime": start, "endTime": end}
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def load_raw_table(cursor, table_name: str, fetch_date: str, payload: str) -> None:
    safe_name = table_name.upper().replace("-", "_")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS RAW_{safe_name} (
            fetch_date DATE,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            payload VARIANT
        )
        """
    )
    cursor.execute(
        f"""
        INSERT INTO RAW_{safe_name} (fetch_date, payload)
        SELECT %s, PARSE_JSON(%s)
        """,
        (fetch_date, payload),
    )


def main():
    with open_dagster_pipes() as pipes:
        target_date = pipes.get_extra("target_date") or str(
            (datetime.now(timezone.utc) - timedelta(days=1)).date()
        )
        start = f"{target_date}T00:00:00Z"
        end = f"{target_date}T23:59:59Z"
        pipes.log.info(f"Fetching Google Health data for {target_date}")

        credentials = get_credentials()
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {credentials.token}"})

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        rows_loaded = {}
        try:
            for data_type in DATA_TYPES:
                pipes.log.info(f"Fetching data type: {data_type}")
                data = fetch_data_type(session, data_type, start, end)
                load_raw_table(cursor, data_type, target_date, str(data).replace("'", "''"))
                rows_loaded[data_type] = 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            pipes.log.error(f"Google Health extraction failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        pipes.report_asset_materialization(
            metadata={
                "target_date": target_date,
                "data_types_loaded": list(rows_loaded.keys()),
            },
        )


if __name__ == "__main__":
    main()