import json
import subprocess
import sys
import tempfile

from dagster_pipes import open_dagster_pipes


def run_soda_scan(data_source: str, checks_file: str, results_path: str) -> int:
    """Ejecuta soda scan y guarda el resultado en JSON para poder parsearlo."""
    cmd = [
        "soda",
        "scan",
        "-d",
        data_source,
        "-c",
        "configuration.yml",
        checks_file,
        "-srf",
        results_path,
    ]
    process = subprocess.run(cmd, capture_output=True, text=True)
    print(process.stdout)
    if process.stderr:
        print(process.stderr, file=sys.stderr)
    return process.returncode


def summarize_results(results_path: str) -> dict:
    try:
        with open(results_path) as f:
            scan_results = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"checks_passed": None, "checks_failed": None}

    checks = scan_results.get("checks", [])
    passed = sum(1 for c in checks if c.get("outcome") == "pass")
    failed = sum(1 for c in checks if c.get("outcome") == "fail")
    return {
        "checks_total": len(checks),
        "checks_passed": passed,
        "checks_failed": failed,
        "failed_checks": [c["name"] for c in checks if c.get("outcome") == "fail"],
    }


def main():
    with open_dagster_pipes() as pipes:
        data_source = pipes.get_extra("data_source")
        checks_file = pipes.get_extra("checks_file")
        pipes.log.info(f"Running Soda scan: data_source={data_source}, checks_file={checks_file}")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            results_path = tmp.name

        return_code = run_soda_scan(data_source, checks_file, results_path)
        summary = summarize_results(results_path)

        pipes.report_asset_materialization(metadata=summary)

        if return_code != 0 or summary.get("checks_failed"):
            raise Exception(
                f"Soda scan failed ({checks_file}): {summary.get('failed_checks')}"
            )


if __name__ == "__main__":
    main()