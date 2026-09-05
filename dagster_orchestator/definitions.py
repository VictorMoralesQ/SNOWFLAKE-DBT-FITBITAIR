import os

from dagster import AssetExecutionContext, Definitions, asset
from dagster_docker import PipesDockerClient


SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
    "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
    "SNOWFLAKE_PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
    "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
    "SNOWFLAKE_DATABASE": os.environ["SNOWFLAKE_DATABASE"],
    "SNOWFLAKE_SCHEMA": os.environ["SNOWFLAKE_SCHEMA"],
}
GOOGLE_ENV = {
    "GOOGLE_CLIENT_ID": os.environ["GOOGLE_CLIENT_ID"],
    "GOOGLE_CLIENT_SECRET": os.environ["GOOGLE_CLIENT_SECRET"],
    "GOOGLE_REFRESH_TOKEN": os.environ["GOOGLE_REFRESH_TOKEN"],
}


@asset
def fitbit_raw(context: AssetExecutionContext, docker_pipes_client: PipesDockerClient):
    """Extrae datos de la Google Health API y los carga en la capa RAW de Snowflake."""
    return docker_pipes_client.run(
        image="fitbit-extractor:latest",
        command=["python", "fetch_data.py"],
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