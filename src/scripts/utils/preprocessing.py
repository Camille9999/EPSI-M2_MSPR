"""Shared preprocessing helpers used by the silver build pipeline and the API.

Kept side-effect free so that artefacts pickled with references to functions
defined here (e.g. ``FunctionTransformer(_forward_fill_impute)``) can be
re-loaded by any consumer that simply imports ``src.preprocessing``.
"""

from __future__ import annotations

import pandas as pd


def _forward_fill_impute(X):
    """Fill missing values by forward fill (carry the last valid observation
    forward), then backward fill as a safety net for leading NaNs at the very
    start of the series."""
    frame = X if isinstance(X, pd.DataFrame) else pd.DataFrame(X)
    return frame.ffill().bfill().to_numpy()
