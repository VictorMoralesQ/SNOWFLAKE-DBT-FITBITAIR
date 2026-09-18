import json
import subprocess
import sys
from pathlib import Path

from dagster_pipes import open_dagster_pipes

RUN_RESULTS_PATH = Path("target/run_results.json")


def run_dbt() -> int:
    process = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        capture_output=True,
        text=True,
    )
    print(process.stdout)
    if process.stderr:
        print(process.stderr, file=sys.stderr)
    return process.returncode


def summarize_run_results() -> dict:
    """Lee target/run_results.json (generado por dbt) para sacar detalle por modelo."""
    if not RUN_RESULTS_PATH.exists():
        return {"models_run": None}

    with open(RUN_RESULTS_PATH) as f:
        run_results = json.load(f)

    results = run_results.get("results", [])
    by_status = {}
    for r in results:
        status = r.get("status", "unknown")
        by_status[status] = by_status.get(status, 0) + 1

    failed_models = [
        r["unique_id"] for r in results if r.get("status") not in ("success", "pass")
    ]

    return {
        "models_total": len(results),
        "status_breakdown": by_status,
        "failed_models": failed_models,
        "elapsed_seconds": run_results.get("elapsed_time"),
    }


def main():
    with open_dagster_pipes() as pipes:
        pipes.log.info("Running dbt run")

        return_code = run_dbt()
        summary = summarize_run_results()

        pipes.report_asset_materialization(metadata=summary)

        if return_code != 0 or summary.get("failed_models"):
            raise Exception(f"dbt run failed. Failed models: {summary.get('failed_models')}")


if __name__ == "__main__":
    main()