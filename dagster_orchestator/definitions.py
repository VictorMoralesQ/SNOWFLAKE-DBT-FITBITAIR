import os
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset
from dagster_docker import PipesDockerClient

SECRETS_DIR = Path("secrets")


def load_secrets() -> dict[str, str]:
    """Read all secrets/*.txt files into a dict."""
    secrets = {}
    if SECRETS_DIR.is_dir():
        for file in SECRETS_DIR.glob("*.txt"):
            secrets[file.stem.upper()] = file.read_text().strip()
    return secrets


def get_secret(name: str, secrets: dict[str, str]) -> str:
    """Prefer the file-based secret, falling back to the environment."""
    return secrets[name] if name in secrets else os.environ[name]


secrets = load_secrets()

SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
    "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
    "SNOWFLAKE_PASSWORD": get_secret("SNOWFLAKE_PASSWORD", secrets),
    "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
    "SNOWFLAKE_DATABASE": os.environ["SNOWFLAKE_DATABASE"],
    "SNOWFLAKE_ROLE": os.environ["SNOWFLAKE_ROLE"],
    "SNOWFLAKE_SCHEMA_RAW": os.environ["SNOWFLAKE_SCHEMA_RAW"],
    "SNOWFLAKE_SCHEMA_SILVER": os.environ["SNOWFLAKE_SCHEMA_SILVER"],
    "SNOWFLAKE_SCHEMA_GOLD": os.environ["SNOWFLAKE_SCHEMA_GOLD"],
}
GOOGLE_ENV = {
    "GOOGLE_CLIENT_ID": get_secret("GOOGLE_CLIENT_ID", secrets),
    "GOOGLE_CLIENT_SECRET": get_secret("GOOGLE_CLIENT_SECRET", secrets),
    "GOOGLE_REFRESH_TOKEN": get_secret("GOOGLE_REFRESH_TOKEN", secrets),
}


@asset
def fitbit_raw(context: AssetExecutionContext, docker_pipes_client: PipesDockerClient):
    """Extrae datos de la Google Health API y los carga en la capa RAW de Snowflake."""
    return docker_pipes_client.run(
        image="fitbit-extractor:latest",
        command=["python", "raw_data.py"],
        env={**SNOWFLAKE_ENV, **GOOGLE_ENV},
        context=context,
    ).get_results()


@asset(deps=[fitbit_raw])
def soda_check_raw(context: AssetExecutionContext, docker_pipes_client: PipesDockerClient):
    """Valida la calidad de los datos RAW antes de transformarlos con dbt."""
    return docker_pipes_client.run(
        image="soda-runner:latest",
        command=["python", "run_with_pipes.py"],
        env=SNOWFLAKE_ENV,
        extras={"checks_file": "raw_checks.yml", "data_source": "my_ds"},
        context=context,
    ).get_results()


@asset(deps=[soda_check_raw])
def dbt_run(context: AssetExecutionContext, docker_pipes_client: PipesDockerClient):
    """Ejecuta las transformaciones dbt (staging -> marts, modelo estrella)."""
    return docker_pipes_client.run(
        image="dbt-runner:latest",
        command=["python", "run_with_pipes.py"],
        env=SNOWFLAKE_ENV,
        context=context,
    ).get_results()


@asset(deps=[dbt_run])
def soda_check_marts(context: AssetExecutionContext, docker_pipes_client: PipesDockerClient):
    """Valida la calidad de los datos en las tablas finales (marts)."""
    return docker_pipes_client.run(
        image="soda-runner:latest",
        command=["python", "run_with_pipes.py"],
        env=SNOWFLAKE_ENV,
        extras={"checks_file": "marts_checks.yml", "data_source": "my_ds"},
        context=context,
    ).get_results()


defs = Definitions(
    assets=[fitbit_raw, soda_check_raw, dbt_run, soda_check_marts],
    resources={
        "docker_pipes_client": PipesDockerClient(),
    },
)