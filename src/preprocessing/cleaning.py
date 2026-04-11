import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

def standardize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # date_time
    time_col = "timestamp"
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["year"] = df[time_col].dt.year
    df["month"] = df[time_col].dt.month
    df["day"] = df[time_col].dt.day
    df["hour"] = df[time_col].dt.hour
    df["weekday"] = df[time_col].dt.dayofweek
    df["quarter"] = df[time_col].dt.quarter

    df["is_weekend"] = df[time_col].dt.dayofweek >= 5
    df["is_month_end"] = df[time_col].dt.is_month_end
    df["days_since_ref"] = (df["time_col"] - pd.Timestamp("1999-03-15")).dt.days

    # text
    text_cols = ["title", "text"]


