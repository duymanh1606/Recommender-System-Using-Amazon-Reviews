import pandas as pd
import numpy as np
import re

def handle_miss_value(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    m_cfg = cfg.get("missing_values", {})
    if not m_cfg:
        return df

    # Xóa hàng dựa trên danh sách cột bắt buộc
    drop_cols = m_cfg.get("drop_is_null", [])
    df = df.dropna(subset=[c for c in drop_cols if c in df.columns])

    # Điền giá trị cố định
    fill_vals = m_cfg.get("fill_value", {})
    for col, value in fill_vals.items():
        if col in df.columns:
            df[col] = df[col].fillna(value)

    return df

def handle_numerical_value(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    num_cfg = cfg.get("numerical_values", {})
    num_cols = num_cfg.get("numerical_columns", [])

    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            if num_cfg.get("non_negative", False):
                df.loc[df[col] < 0, col] = 0

            cap_min = num_cfg.get("min_num")
            cap_max = num_cfg.get("max_num")
            if cap_min is not None or cap_max is not None:
                df[col] = df[col].clip(lower=cap_min, upper=cap_max)
    return df


def handle_date(df: pd.DataFrame, current_cfg: dict) -> pd.DataFrame:
    # Lấy tên cột thời gian từ config
    time_col = current_cfg.get('time_cols', [])

    for col in time_col:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df["year"] = df[col].dt.year
        df["month"] = df[col].dt.month
        df["day"] = df[col].dt.day
        df["hour"] = df[col].dt.hour
        df["weekday"] = df[col].dt.dayofweek
        df["quarter"] = df[col].dt.quarter

        df["is_weekend"] = df[col].dt.dayofweek >= 5
        df["is_month_end"] = df[col].dt.is_month_end

        ref_date = pd.Timestamp("1999-03-15")
        df["days_since_ref"] = (df[col] - ref_date).dt.days

    return df

def handle_text(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    text_cols = cfg.get('text_cols', [])
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
            df[col] = df[col].str.replace(r"[^\w\s]", "", regex=True)
    return df


def apply_cleaning_pipeline(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    processed_df = df.copy()

    return (processed_df
            .pipe(handle_text, cfg)
            .pipe(handle_numerical_value, cfg)
            .pipe(handle_date, cfg)
            .pipe(handle_miss_value, cfg))