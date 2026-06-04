import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

import joblib
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeStreamlitModel:
    def __init__(self):
        dates = pd.date_range("2024-01-01", periods=140, freq="D")
        values = pd.Series(
            [50000.0 + index * 10.0 for index in range(len(dates))],
            index=dates,
        )
        self.fittedvalues = values - 50.0
        self.resid = values - self.fittedvalues
        self.model = FakeStreamlitModelData(dates, values)

    def predict(self, start, end, dynamic=True):
        dates = pd.DatetimeIndex(self.model.data.dates)[start:end + 1]
        return pd.Series(
            [50000.0 + index * 10.0 for index in range(start, end + 1)],
            index=dates,
        )


class FakeStreamlitModelData:
    def __init__(self, dates, values):
        self.data = FakeStreamlitModelDates(dates)
        self.endog = values.to_numpy().reshape(-1, 1)


class FakeStreamlitModelDates:
    def __init__(self, dates):
        self.dates = dates


def _free_port() -> int:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])
    except PermissionError:
        pytest.skip("Socket creation is not permitted in this environment.")


def _write_streamlit_registry(models_dir: Path) -> str:
    run_id = "e2e_run_20240101"
    models_dir.mkdir(parents=True)
    run_dir = models_dir / run_id
    run_dir.mkdir()

    joblib.dump(FakeStreamlitModel(), run_dir / "model.pkl")

    registry = {
        "latest_run_id": run_id,
        "n_runs": 1,
        "runs": [
            {
                "run_id": run_id,
                "run_dir": run_id,
                "trained_at": "2024-01-06T00:00:00Z",
                "training_start": "2024-01-01",
                "training_end": "2024-01-05",
                "n_training_days": 5,
                "features": ["temp_pc_01", "production_mw_lag1"],
                "order": [1, 0, 0],
                "seasonal_order": [0, 0, 0, 0],
                "model": {
                    "model_file": "model.pkl",
                    "scaler_file": "scaler.pkl",
                    "metadata_file": "metadata.json",
                    "insample_metrics": {
                        "insample_MAE_MW": 1.2,
                        "insample_RMSE_MW": 1.5,
                        "insample_MAPE_pct": 0.1,
                    },
                },
            }
        ],
    }
    (models_dir / "sarima_metadata.json").write_text(
        json.dumps(registry),
        encoding="utf-8",
    )
    return run_id


def _assert_no_streamlit_exception(page, expect_func) -> None:
    forbidden_texts = [
        "Traceback",
        "ModuleNotFoundError",
        "ImportError",
        "NameError",
        "TypeError",
        "ValueError",
        "KeyError",
        "FileNotFoundError",
        "Uncaught app exception",
    ]
    for text in forbidden_texts:
        expect_func(page.get_by_text(text)).to_have_count(0)


def _wait_for_http(url: str, process: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise AssertionError(f"Streamlit exited before serving the page:\n{output}")

        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.25)

    raise AssertionError(f"Streamlit did not answer at {url} within {timeout} seconds")


def test_streamlit_homepage_renders_registered_runs_with_playwright(tmp_path):
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import expect, sync_playwright
    except ImportError:
        pytest.skip("Playwright is not installed.")

    models_dir = tmp_path / "models"
    run_id = _write_streamlit_registry(models_dir)
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    print(f"Streamlit E2E URL: {url}")

    env = {
        **os.environ,
        "MODELS_DIR": str(models_dir),
        "API_URL": "http://127.0.0.1:8000",
        "PYTHONPATH": f"{PROJECT_ROOT}:{Path(__file__).resolve().parent}",
    }
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(PROJECT_ROOT / "src" / "frontend" / "Accueil.py"),
        f"--server.port={port}",
        "--server.address=127.0.0.1",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_http(url, process)

        try:
            with sync_playwright() as playwright:
                debug_enabled = os.getenv("PWDEBUG") == "1"
                headless = (
                    False
                    if debug_enabled
                    else os.getenv("PLAYWRIGHT_HEADLESS", "1").lower()
                    not in {"0", "false", "no"}
                )
                slow_mo = int(os.getenv("PLAYWRIGHT_SLOW_MO", "0"))

                browser = playwright.chromium.launch(
                    headless=headless,
                    slow_mo=slow_mo,
                )
                page = browser.new_page()
                page.goto(url, wait_until="domcontentloaded")
                if debug_enabled:
                    page.pause()

                expect(page.get_by_text("SARIMA Monitoring")).to_be_visible()
                expect(page.get_by_text("Runs disponibles")).to_be_visible()
                expect(page.get_by_text(run_id).first).to_be_visible()
                _assert_no_streamlit_exception(page, expect)

                pages = [
                    ("Comparatif", "Comparatif des runs"),
                    ("Evolution", "Évolution temporelle des métriques"),
                    ("Analyse", "Analyse détaillée d'un run"),
                    ("Recursif", "Prévisions récursives"),
                    ("API", "Statut de l'API de prédiction"),
                ]
                for nav_label, expected_title in pages:
                    page.get_by_role("link", name=nav_label).click()
                    expect(page.get_by_text(expected_title)).to_be_visible()
                    _assert_no_streamlit_exception(page, expect)

                browser.close()
        except PlaywrightError as exc:
            if "Executable doesn't exist" in str(exc):
                pytest.skip("Playwright Chromium is not installed.")
            raise
    finally:
        process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
