"""Page 3 — Analyse détaillée d'un run (fitted, résidus, drift, saisonnalité)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from scipy import stats as sp_stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loader import (
    WARMUP_DAYS,
    _resolve_model_entry,
    build_insample_df,
    build_ts_series,
    get_residuals,
    list_runs,
)

st.set_page_config(page_title="Analyse d'un run", page_icon="🔍", layout="wide")
st.title("🔍 Analyse détaillée d'un run")

WEEKDAY_LABELS = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
MONTH_LABELS = ["Jan", "Fév", "Mar", "Avr", "Mai", "Jun", "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"]

# ── Sélection du run ──────────────────────────────────────────────────────────
runs = list_runs()
if not runs:
    st.warning("Aucun run disponible.")
    st.stop()

run_options = [r["run_id"] for r in runs]
run_id = st.sidebar.selectbox("Run", run_options)
run_meta = next(r for r in runs if r["run_id"] == run_id)

st.sidebar.markdown(f"**Entraîné le :** {run_meta.get('trained_at', '—')}")
st.sidebar.markdown(f"**Features :** {', '.join(run_meta.get('features', []))}")
st.sidebar.markdown(
    f"**Ordres :** {tuple(run_meta.get('order', []))} × {tuple(run_meta.get('seasonal_order', []))}"
)
ROLLING_WINDOW = st.sidebar.slider("Fenêtre rolling (jours)", 7, 90, 30)
alert_factor = st.sidebar.slider("Facteur d'alerte (× baseline)", 1.1, 3.0, 2.0, step=0.1)

# ── Chargement ────────────────────────────────────────────────────────────────
with st.spinner("Chargement du modèle…"):
    df = build_insample_df(run_id)
    ts = build_ts_series(run_id)
    resid = get_residuals(run_id)

if df.empty:
    st.error("Impossible de charger les artefacts du run sélectionné.")
    st.stop()

m_meta = _resolve_model_entry(run_meta).get("insample_metrics", {})

# ── Tabs ──────────────────────────────────────────────────────────────────────
tabs = st.tabs([
    "📊 Vue d'ensemble",
    "📉 Fitted vs Réel",
    "📏 Distribution des erreurs",
    "🌊 Stabilité / Drift",
    "🗓 Saisonnalité",
    "🔬 Résidus",
])

# ══ Tab 1 — Vue d'ensemble ═════════════════════════════════════════════════════
with tabs[0]:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MAE (MW)", f"{m_meta['insample_MAE_MW']:.1f}" if m_meta.get("insample_MAE_MW") else "—")
    c2.metric("RMSE (MW)", f"{m_meta['insample_RMSE_MW']:.1f}" if m_meta.get("insample_RMSE_MW") else "—")
    c3.metric("MAPE (%)", f"{m_meta['insample_MAPE_pct']:.3f}" if m_meta.get("insample_MAPE_pct") else "—")
    c4.metric("n jours (post-warmup)", len(df))

    st.subheader("Statistiques de base")
    st.dataframe(ts.describe().rename("consommation_mw").round(1).to_frame().T, width="stretch")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts.index, y=ts.values, mode="lines", name="Consommation (MW)",
        line=dict(color="#1f77b4", width=0.8),
    ))
    rolling = ts.rolling(30, min_periods=15).mean()
    fig.add_trace(go.Scatter(
        x=rolling.index, y=rolling.values, mode="lines",
        name="Moy. mobile 30j", line=dict(color="#ff7f0e", width=2),
    ))
    fig.update_layout(
        title="Série temporelle complète",
        xaxis_title="Date", yaxis_title="Consommation (MW)", hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch", key="tab1_ts")

# ══ Tab 2 — Fitted vs Réel ═════════════════════════════════════════════════════
with tabs[1]:
    zoom = st.slider("Zoom — derniers N jours", 30, len(df), min(180, len(df)), step=10)

    for idx, (title, d) in enumerate([
        (f"Zoom — {zoom} derniers jours", df.iloc[-zoom:]),
        ("Série complète (post-warmup)", df),
    ]):
        mae_val = d["abs_error"].mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=d.index, y=d["actual"], mode="lines", name="Réel",
            line=dict(color="#1f77b4", width=0.9),
        ))
        fig.add_trace(go.Scatter(
            x=d.index, y=d["fitted"], mode="lines", name="Fitted",
            line=dict(color="#d62728", width=0.9, dash="dash"),
        ))
        fig.update_layout(
            title=f"{title}  —  MAE = {mae_val:.0f} MW",
            xaxis_title="Date", yaxis_title="MW", hovermode="x unified",
        )
        st.plotly_chart(fig, width="stretch", key=f"tab2_fitted_{idx}")

# ══ Tab 3 — Distribution ══════════════════════════════════════════════════════
with tabs[2]:
    col1, col2 = st.columns(2)
    with col1:
        fig = px.histogram(
            df, x="abs_error", nbins=60, marginal="box",
            title="|Erreur| absolue in-sample",
            labels={"abs_error": "|Erreur| (MW)"},
            color_discrete_sequence=["#2ca02c"],
        )
        st.plotly_chart(fig, width="stretch", key="tab3_hist")
    with col2:
        fig = px.box(
            df.reset_index(names="date"), x="month", y="abs_error",
            title="|Erreur| par mois",
            labels={"month": "Mois", "abs_error": "|Erreur| (MW)"},
            color_discrete_sequence=["#9ecae1"],
        )
        fig.update_xaxes(tickvals=list(range(1, 13)), ticktext=MONTH_LABELS)
        st.plotly_chart(fig, width="stretch", key="tab3_box_month")

    st.subheader("Statistiques")
    st.dataframe(
        df["abs_error"].describe().rename("|Erreur| (MW)").round(1).to_frame().T,
        width="stretch",
    )

# ══ Tab 4 — Stabilité / Drift ═════════════════════════════════════════════════
with tabs[3]:
    df_sorted = df.sort_index()
    min_periods = ROLLING_WINDOW // 2
    rolling_mae = (
        df_sorted["abs_error"]
        .rolling(ROLLING_WINDOW, min_periods=min_periods)
        .mean()
        .iloc[ROLLING_WINDOW:]
    )
    rolling_rmse = (
        np.sqrt(
            df_sorted["sq_error"]
            .rolling(ROLLING_WINDOW, min_periods=min_periods)
            .mean()
        )
        .iloc[ROLLING_WINDOW:]
    )

    b_mae = m_meta.get("insample_MAE_MW")
    b_rmse = m_meta.get("insample_RMSE_MW")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=rolling_mae.index, y=rolling_mae.values, mode="lines",
        name=f"Rolling MAE ({ROLLING_WINDOW}j)", line=dict(color="#2ca02c", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=rolling_rmse.index, y=rolling_rmse.values, mode="lines",
        name=f"Rolling RMSE ({ROLLING_WINDOW}j)", line=dict(color="#9467bd", width=2),
    ))
    if b_mae:
        fig.add_hline(
            y=b_mae, line_dash="dash", line_color="#2ca02c", opacity=0.5,
            annotation_text=f"MAE baseline ({b_mae:.0f} MW)",
        )
        fig.add_hline(
            y=b_mae * alert_factor, line_dash="dot", line_color="#d62728",
            annotation_text=f"Seuil alerte ×{alert_factor:.1f} ({b_mae * alert_factor:.0f} MW)",
        )
    if b_rmse:
        fig.add_hline(
            y=b_rmse, line_dash="dash", line_color="#9467bd", opacity=0.5,
            annotation_text=f"RMSE baseline ({b_rmse:.0f} MW)",
        )
    fig.update_layout(
        title=f"Stabilité temporelle  (rolling {ROLLING_WINDOW}j)",
        xaxis_title="Date", yaxis_title="MW", hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch", key="tab4_drift")

    if b_mae and not rolling_mae.empty:
        above = (rolling_mae > b_mae * alert_factor).sum()
        pct = above / len(rolling_mae) * 100
        if above > 0:
            st.warning(
                f"⚠️ {above} jours ({pct:.1f}%) au-dessus du seuil "
                f"(MAE rolling > {b_mae * alert_factor:.0f} MW)"
            )
        else:
            st.success("✅ Aucune dérive détectée — rolling MAE sous le seuil d'alerte sur toute la période.")

# ══ Tab 5 — Saisonnalité ══════════════════════════════════════════════════════
with tabs[4]:
    pivot = df.pivot_table(index="month", columns="dow", values="abs_error", aggfunc="mean")
    pivot.columns = [WEEKDAY_LABELS[i] for i in pivot.columns]
    pivot.index = [MONTH_LABELS[m - 1] for m in pivot.index]

    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale="YlOrRd",
            text=[[f"{v:.0f}" for v in row] for row in pivot.values],
            texttemplate="%{text}",
            showscale=True,
            colorbar=dict(title="|Erreur| moy. (MW)"),
        )
    )
    fig.update_layout(
        title="|Erreur| moyenne  (mois × jour de semaine)",
        xaxis_title="Jour de semaine",
        yaxis_title="Mois",
    )
    st.plotly_chart(fig, width="stretch", key="tab5_heatmap")

    col1, col2 = st.columns(2)
    with col1:
        fig2 = px.box(
            df.reset_index(names="date"), x="dow", y="abs_error",
            title="|Erreur| par jour de semaine",
            labels={"dow": "Jour", "abs_error": "|Erreur| (MW)"},
            color_discrete_sequence=["#9ecae1"],
        )
        fig2.update_xaxes(tickvals=list(range(7)), ticktext=WEEKDAY_LABELS)
        st.plotly_chart(fig2, width="stretch", key="tab5_box_dow")
    with col2:
        fig3 = px.box(
            df.reset_index(names="date"), x="month", y="abs_error",
            title="|Erreur| par mois",
            labels={"month": "Mois", "abs_error": "|Erreur| (MW)"},
            color_discrete_sequence=["#9ecae1"],
        )
        fig3.update_xaxes(tickvals=list(range(1, 13)), ticktext=MONTH_LABELS)
        st.plotly_chart(fig3, width="stretch", key="tab5_box_month")

# ══ Tab 6 — Résidus ═══════════════════════════════════════════════════════════
with tabs[5]:
    if resid.empty:
        st.error("Impossible de charger les résidus.")
    else:
        mu = float(resid.mean())
        sigma = float(resid.std())
        pct_1s = float((resid.abs() <= sigma).mean() * 100)
        pct_2s = float((resid.abs() <= 2 * sigma).mean() * 100)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Moyenne (bias)", f"{mu:+.1f} MW")
        c2.metric("Std (σ)", f"{sigma:.1f} MW")
        c3.metric("Skewness", f"{resid.skew():.3f}")
        c4.metric("Kurtosis", f"{resid.kurtosis():.3f}")

        col1, col2 = st.columns(2)
        with col1:
            x_range = np.linspace(resid.min(), resid.max(), 200)
            fig = px.histogram(
                resid, nbins=50, histnorm="probability density",
                title="Distribution des résidus",
                labels={"value": "Résidu (MW)"},
                color_discrete_sequence=["#2ca02c"],
            )
            fig.add_trace(go.Scatter(
                x=x_range, y=sp_stats.norm.pdf(x_range, mu, sigma),
                mode="lines", name=f"N({mu:.0f}, {sigma:.0f})",
                line=dict(color="#d62728", width=2),
            ))
            st.plotly_chart(fig, width="stretch", key="tab6_resid_hist")

        with col2:
            (osm, osr), (slope, intercept, _) = sp_stats.probplot(resid, dist="norm")
            fig_qq = go.Figure()
            fig_qq.add_trace(go.Scatter(
                x=osm, y=osr, mode="markers", name="Résidus",
                marker=dict(color="#1f77b4", size=4),
            ))
            fig_qq.add_trace(go.Scatter(
                x=[min(osm), max(osm)],
                y=[slope * min(osm) + intercept, slope * max(osm) + intercept],
                mode="lines", name="Normale théorique",
                line=dict(color="#d62728", dash="dash"),
            ))
            fig_qq.update_layout(
                title="Q-Q plot des résidus",
                xaxis_title="Quantiles théoriques",
                yaxis_title="Quantiles observés",
            )
            st.plotly_chart(fig_qq, width="stretch", key="tab6_qq")

        # ACF / PACF
        from statsmodels.tsa.stattools import acf
        from statsmodels.tsa.stattools import pacf as pacf_func

        nlags = 40
        acf_vals, acf_ci = acf(resid, nlags=nlags, alpha=0.05, fft=True)
        pacf_vals, pacf_ci = pacf_func(resid, nlags=nlags, alpha=0.05, method="ywm")
        lags = list(range(len(acf_vals)))

        col1, col2 = st.columns(2)
        for col, vals, confint, title, chart_key in [
            (col1, acf_vals, acf_ci, "ACF des résidus", "tab6_acf"),
            (col2, pacf_vals, pacf_ci, "PACF des résidus", "tab6_pacf"),
        ]:
            with col:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=lags, y=confint[:, 1], mode="lines",
                    line=dict(color="rgba(31,119,180,0)", width=0), showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=lags, y=confint[:, 0], mode="lines",
                    fill="tonexty", fillcolor="rgba(31,119,180,0.15)",
                    line=dict(color="rgba(31,119,180,0)", width=0), name="IC 95%",
                ))
                for lag, val in zip(lags, vals):
                    fig.add_shape(
                        type="line", x0=lag, x1=lag, y0=0, y1=val,
                        line=dict(color="#1f77b4", width=2),
                    )
                fig.add_trace(go.Scatter(
                    x=lags, y=vals, mode="markers",
                    marker=dict(color="#1f77b4", size=5), name=title.split()[0],
                ))
                fig.add_hline(y=0, line_color="black", line_width=0.5)
                fig.update_layout(
                    title=title, xaxis_title="Lag", yaxis_title="Corrélation",
                )
                col.plotly_chart(fig, width="stretch", key=chart_key)

        st.info(
            f"Dans ±1σ : **{pct_1s:.1f}%** (attendu ~68.3%)  |  "
            f"Dans ±2σ : **{pct_2s:.1f}%** (attendu ~95.5%)"
        )
