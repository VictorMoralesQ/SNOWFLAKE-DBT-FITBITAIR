from dagster import asset, Definitions, AssetExecutionContext
from dagster_docker import PipesDockerClient

docker_client = PipesDockerClient()

@asset
def fitbit_raw(context: AssetExecutionContext):
    return docker_client.run(
        image="fitbit-extractor:latest",
        command=["python", "fetch_fitbit.py"],
        env={"SNOWFLAKE_ACCOUNT": "...", "FITBIT_TOKEN": "..."},
        context=context,
    ).get_results()

@asset(deps=[fitbit_raw])
def soda_check_raw(context: AssetExecutionContext):
    return docker_client.run(
        image="soda-runner:latest",
        command=["soda", "scan", "-d", "my_ds", "raw_checks.yml"],
        context=context,
    ).get_results()

@asset(deps=[soda_check_raw])
def dbt_run(context: AssetExecutionContext):
    return docker_client.run(
        image="dbt-runner:latest",
        command=["dbt", "run"],
        context=context,
    ).get_results()

@asset(deps=[dbt_run])
def soda_check_marts(context: AssetExecutionContext):
    return docker_client.run(
        image="soda-runner:latest",
        command=["soda", "scan", "-d", "my_ds", "marts_checks.yml"],
        context=context,
    ).get_results()

defs = Definitions(assets=[fitbit_raw, soda_check_raw, dbt_run, soda_check_marts])