"""
shap_explainer.py
=================
Generates SHAP waterfall charts for individual break rows.
Renders as a Plotly Figure (NOT matplotlib) for Streamlit compatibility.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def explain_break(
    explainer,
    feature_row: pd.Series,
    feature_names: list,
    break_id: Optional[str] = None,
    false_break_prob: Optional[float] = None,
) -> go.Figure:
    """
    Generate a SHAP waterfall chart for a single break row.

    Parameters
    ----------
    explainer     : shap.TreeExplainer
    feature_row   : pd.Series with feature values (indexed by feature name)
    feature_names : list of feature column names (in model order)
    break_id      : displayed in chart title
    false_break_prob : displayed in chart title

    Returns
    -------
    Plotly go.Figure of the waterfall chart.
    """
    import shap

    # Build input array for the explainer
    X = _build_input_array(feature_row, feature_names)

    # Compute SHAP values
    shap_values = explainer(X)

    # For binary classifier take class 1 (false_break)
    if hasattr(shap_values, "values"):
        vals = shap_values.values[0]
        if vals.ndim == 2:
            vals = vals[:, 1]
        base = float(shap_values.base_values[0]) if shap_values.base_values.ndim == 1 \
               else float(shap_values.base_values[0][1])
    else:
        vals = shap_values[1][0] if isinstance(shap_values, list) else shap_values[0]
        base = float(explainer.expected_value[1]) if isinstance(
            explainer.expected_value, (list, np.ndarray)
        ) else float(explainer.expected_value)

    # Pair names with SHAP values
    names  = feature_names[:len(vals)]
    values = vals[:len(names)]

    # Sort by absolute contribution (descending)
    sorted_idx = np.argsort(np.abs(values))[::-1]
    top_n = min(15, len(sorted_idx))
    idx   = sorted_idx[:top_n]

    top_names  = [names[i] for i in idx]
    top_values = [float(values[i]) for i in idx]
    top_data   = [float(feature_row.get(n, np.nan)) for n in top_names]

    fig = _build_waterfall_figure(
        top_names, top_values, top_data, base,
        break_id=break_id, false_break_prob=false_break_prob,
    )
    return fig


def get_top_features(
    shap_values_array,
    feature_names: list,
    n: int = 5,
) -> list:
    """
    Return top-n (feature_name, shap_value) tuples by absolute contribution.

    Parameters
    ----------
    shap_values_array : 1-D array of SHAP values for a single row
    feature_names     : list of feature names
    n                 : number of top features to return
    """
    if hasattr(shap_values_array, "values"):
        vals = np.array(shap_values_array.values).ravel()
    else:
        vals = np.array(shap_values_array).ravel()

    vals  = vals[:len(feature_names)]
    names = feature_names[:len(vals)]

    sorted_idx = np.argsort(np.abs(vals))[::-1][:n]
    return [(names[i], float(vals[i])) for i in sorted_idx]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_input_array(feature_row: pd.Series, feature_names: list) -> np.ndarray:
    """Construct a (1, n_features) float32 array from a feature Series."""
    row = []
    for name in feature_names:
        v = feature_row.get(name, np.nan)
        try:
            row.append(float(v))
        except (TypeError, ValueError):
            row.append(np.nan)
    return np.array(row, dtype=np.float32).reshape(1, -1)


def _build_waterfall_figure(
    names: list,
    values: list,
    data: list,
    base_value: float,
    break_id: Optional[str] = None,
    false_break_prob: Optional[float] = None,
) -> go.Figure:
    """Build a Plotly waterfall chart from SHAP contributions."""
    # Colours: positive (pushes toward false break) = red, negative = green
    colours = [
        "rgba(220,53,69,0.75)" if v >= 0 else "rgba(40,167,69,0.75)"
        for v in values
    ]

    hover_texts = [
        f"<b>{n}</b><br>Value: {d:.4g}<br>SHAP: {v:+.4f}"
        for n, v, d in zip(names, values, data)
    ]

    # Waterfall bars
    measures = ["relative"] * len(names) + ["total"]
    x_labels = [
        f"{n}<br><sub>({d:.3g})</sub>"
        for n, d in zip(names, data)
    ] + ["False Break Prob"]

    y_values = list(values) + [base_value + sum(values)]

    fig = go.Figure(go.Waterfall(
        name          = "SHAP Contributions",
        orientation   = "v",
        measure       = measures,
        x             = x_labels,
        y             = y_values,
        connector     = {"line": {"color": "rgba(0,0,0,0.2)"}},
        decreasing    = {"marker": {"color": "rgba(40,167,69,0.75)"}},
        increasing    = {"marker": {"color": "rgba(220,53,69,0.75)"}},
        totals        = {"marker": {"color": "rgba(108,117,125,0.85)"}},
        hovertext     = hover_texts + [f"Predicted Prob: {base_value + sum(values):.3f}"],
        hoverinfo     = "text",
    ))

    title = "SHAP Explanation"
    if break_id:
        title += f" — Break {break_id}"
    if false_break_prob is not None:
        title += f" (false_break_prob={false_break_prob:.3f})"

    fig.update_layout(
        title          = title,
        xaxis_title    = "Feature (raw value)",
        yaxis_title    = "SHAP Contribution",
        plot_bgcolor   = "white",
        paper_bgcolor  = "white",
        font           = {"size": 11},
        margin         = {"l": 40, "r": 20, "t": 60, "b": 120},
        height         = 450,
        showlegend     = False,
    )

    # Reference line at base value
    fig.add_hline(
        y=base_value,
        line_dash="dot",
        line_color="grey",
        annotation_text=f"Base value = {base_value:.3f}",
        annotation_position="top right",
    )

    return fig
