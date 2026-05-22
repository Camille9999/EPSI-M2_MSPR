import pandas as pd
import pytest

from src.scripts.train_sarima import (
    _parse_features,
    compute_insample_metrics,
    select_features,
)


class FakeModelResult:
    def __init__(self, fittedvalues):
        self.fittedvalues = fittedvalues


def test_parse_features_removes_empty_values_and_spaces():
    features = _parse_features(" temp_pc_01, ,production_mw_lag1, dow_sin ")

    assert features == ["temp_pc_01", "production_mw_lag1", "dow_sin"]


def test_select_features_returns_requested_columns_in_order():
    exog_all = pd.DataFrame(
        {
            "temp_pc_01": [1.0, 2.0],
            "production_mw_lag1": [100.0, 110.0],
            "dow_sin": [0.0, 0.5],
        }
    )

    result = select_features(exog_all, ["production_mw_lag1", "temp_pc_01"])

    assert list(result.columns) == ["production_mw_lag1", "temp_pc_01"]


def test_select_features_raises_error_for_missing_column():
    exog_all = pd.DataFrame({"temp_pc_01": [1.0, 2.0]})

    with pytest.raises(ValueError, match="Requested feature"):
        select_features(exog_all, ["missing_feature"])


def test_compute_insample_metrics_returns_expected_values():
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    ts = pd.Series([100.0, 200.0, 300.0], index=index)
    fitted = pd.Series([90.0, 210.0, 330.0], index=index)

    metrics = compute_insample_metrics(FakeModelResult(fitted), ts)

    assert metrics == {
        "insample_MAE_MW": 16.67,
        "insample_RMSE_MW": 19.15,
        "insample_MAPE_pct": 8.3333,
    }
