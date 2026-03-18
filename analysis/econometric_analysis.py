
import pandas as pd
import numpy as np
from google.cloud import bigquery
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from statsmodels.tsa.vector_ar.var_model import VAR
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
import warnings
warnings.filterwarnings("ignore")

PROJECT_ID = "arla-media-correlation-2025"
CLIENT = bigquery.Client(project=PROJECT_ID)

# ── Channels to test against Arla brand interest ──────────────────────────────
CHANNELS = [
    "search_youtube_ads",
    "search_instagram_ads",
    "search_facebook_ads",
    "consumer_confidence",
    "food_cpi",
    "avg_temp"
]

# ── 0. Load data ───────────────────────────────────────────────────────────────
def load_data():
    print("Loading mart_correlation_input...")
    df = CLIENT.query("""
        SELECT *
        FROM `arla-media-correlation-2025.dbt_marts.mart_correlation_input`
        ORDER BY period
    """).to_dataframe()
    print(f"  {len(df)} months loaded ({df['period'].min()} to {df['period'].max()})")
    return df


# ── 1. ADF Stationarity Tests ──────────────────────────────────────────────────
def run_adf_tests(df):
    print("\n── Step 1: ADF Stationarity Tests ──")
    variables = ["search_arla"] + CHANNELS
    rows = []
    for var in variables:
        series = df[var].dropna()
        result = adfuller(series, autolag="AIC")
        is_stationary = result[1] < 0.05
        status = "STATIONARY" if is_stationary else "NON-STATIONARY - needs differencing"
        print(f"  {var:30s} p={result[1]:.4f}  {status}")
        rows.append({
            "variable":       var,
            "adf_statistic":  round(result[0], 4),
            "p_value":        round(result[1], 4),
            "is_stationary":  is_stationary,
            "lags_used":      result[2],
            "n_obs":          result[3]
        })
    return pd.DataFrame(rows)


# ── 2. First-difference non-stationary series ─────────────────────────────────
def prepare_differenced(df):
    variables = ["search_arla"] + CHANNELS
    df_diff = df[["period", "year", "month"] + variables].copy()
    for var in variables:
        df_diff[f"{var}_diff"] = df_diff[var].diff()
    df_diff = df_diff.dropna()
    return df_diff


# ── 3. Granger Causality Tests ─────────────────────────────────────────────────
def run_granger_tests(df_diff, max_lag=4):
    print("\n── Step 2: Granger Causality Tests ──")
    rows = []
    for channel in CHANNELS:
        y = df_diff["search_arla_diff"]
        x = df_diff[f"{channel}_diff"]
        data = pd.concat([y, x], axis=1).dropna()
        try:
            results = grangercausalitytests(data, maxlag=max_lag, verbose=False)
            for lag, res in results.items():
                p_value = res[0]["ssr_ftest"][1]
                f_stat  = res[0]["ssr_ftest"][0]
                print(f"  {channel:30s} lag={lag}  F={f_stat:.3f}  p={p_value:.4f}  {'✓ SIGNIFICANT' if p_value <
0.05 else ''}")
                rows.append({
                    "channel":         channel,
                    "lag_months":      lag,
                    "f_statistic":     round(f_stat, 4),
                    "p_value":         round(p_value, 4),
                    "granger_causes":  p_value < 0.05
                })
        except Exception as e:
            print(f"  {channel}: Error — {e}")
    return pd.DataFrame(rows)


# ── 4. VAR Model ───────────────────────────────────────────────────────────────
def run_var_model(df_diff):
    print("\n── Step 3: VAR Model ──")
    var_cols = [
        "search_arla_diff",
        "search_youtube_ads_diff",
        "search_instagram_ads_diff",
        "search_facebook_ads_diff",
        "consumer_confidence_diff"
    ]
    data = df_diff[var_cols].dropna()
    model = VAR(data)

    # Select optimal lag by AIC
    lag_order = model.select_order(maxlags=6)
    best_lag = lag_order.aic
    print(f"  Optimal lag order (AIC): {best_lag}")

    results = model.fit(best_lag)
    print(results.summary())

    # Extract coefficients
    coef_rows = []
    for eq_name in results.names:
        for param_name, coef, pval in zip(
            results.params.index,
            results.params[eq_name],
            results.pvalues[eq_name]
        ):
            coef_rows.append({
                "equation":    eq_name,
                "predictor":   param_name,
                "coefficient": round(coef, 4),
                "p_value":     round(pval, 4),
                "significant": pval < 0.05
            })

    # Impulse Response Function (6-month horizon)
    irf = results.irf(periods=6)
    irf_rows = []
    for i, shock_var in enumerate(var_cols):
        for j, response_var in enumerate(var_cols):
            for period, value in enumerate(irf.irfs[:, j, i]):
                irf_rows.append({
                    "shock_variable":    shock_var.replace("_diff", ""),
                    "response_variable": response_var.replace("_diff", ""),
                    "period_months":     period,
                    "irf_value":         round(float(value), 4)
                })

    return pd.DataFrame(coef_rows), pd.DataFrame(irf_rows)


# ── 5. OLS Regression (first-differenced + lagged X) ─────────────────────────
def run_ols(df_diff):
    print("\n── Step 4: OLS Regression (Granger-optimal lags) ──")
    df_ols = df_diff.copy()

    # Use Granger-optimal lags: YouTube=3, Facebook=4, others=1
    df_ols["search_youtube_ads_lag3"]    = df_ols["search_youtube_ads_diff"].shift(3)
    df_ols["search_facebook_ads_lag4"]   = df_ols["search_facebook_ads_diff"].shift(4)
    df_ols["search_instagram_ads_lag1"]  = df_ols["search_instagram_ads_diff"].shift(1)
    df_ols["consumer_confidence_lag1"]   = df_ols["consumer_confidence_diff"].shift(1)
    df_ols["food_cpi_lag1"]              = df_ols["food_cpi_diff"].shift(1)
    df_ols["avg_temp_lag1"]              = df_ols["avg_temp"].shift(1)
    df_ols["is_summer"]                  = df_ols["month"].isin([6, 7, 8]).astype(int)

    predictors = [
        "search_youtube_ads_lag3",
        "search_facebook_ads_lag4",
        "search_instagram_ads_lag1",
        "consumer_confidence_lag1",
        "food_cpi_lag1",
        "avg_temp_lag1",
        "is_summer"
    ]
    df_ols = df_ols.dropna(subset=["search_arla_diff"] + predictors)

    X = add_constant(df_ols[predictors])
    y = df_ols["search_arla_diff"]

    model = OLS(y, X).fit(cov_type="HC3")
    print(model.summary())

    rows = []
    for var in model.params.index:
        rows.append({
            "predictor":     var,
            "coefficient":   round(model.params[var], 4),
            "std_error":     round(model.bse[var], 4),
            "t_statistic":   round(model.tvalues[var], 4),
            "p_value":       round(model.pvalues[var], 4),
            "significant":   model.pvalues[var] < 0.05,
            "r_squared":     round(model.rsquared, 4),
            "adj_r_squared": round(model.rsquared_adj, 4),
            "n_obs":         int(model.nobs)
        })
    return pd.DataFrame(rows)


# ── Load results to BigQuery ───────────────────────────────────────────────────
def load_to_bigquery(df, table_name):
    table_id = f"{PROJECT_ID}.dbt_marts.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    CLIENT.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"  → Loaded {len(df)} rows into {table_id}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    df = load_data()

    # Step 1 — Stationarity
    df_adf = run_adf_tests(df)
    load_to_bigquery(df_adf, "mart_stationarity_results")

    # Difference the series
    df_diff = prepare_differenced(df)

    # Step 2 — Granger causality
    df_granger = run_granger_tests(df_diff)
    load_to_bigquery(df_granger, "mart_granger_results")

    # Step 3 — VAR model
    df_var_coef, df_irf = run_var_model(df_diff)
    load_to_bigquery(df_var_coef, "mart_var_coefficients")
    load_to_bigquery(df_irf, "mart_impulse_response")

    # Step 4 — OLS
    df_ols = run_ols(df_diff)
    load_to_bigquery(df_ols, "mart_ols_results")

    print("\n✓ All models complete. Tables loaded to dbt_marts.")


if __name__ == "__main__":
    main()
