import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from app.config.anomaly_detection import (
    TIMESTAMP_FORMAT_READABLE,
    TIMESTAMP_FORMAT,
    WINDOW_SIZE,
    TELEMETRIES,
)
from app.utils.model_loader import get_anomaly_detection_model

"""

"""
def extract_window_features(window_df):
    features = {}
    for col in TELEMETRIES:
        series = window_df[col]
        features[f'{col}_mean'] = series.mean()
        features[f'{col}_std'] = series.std()
        features[f'{col}_min'] = series.min()
        features[f'{col}_max'] = series.max()
        features[f'{col}_trend'] = series.iloc[-1] - series.iloc[0]

    return features

"""
Predict anomalies using machine learning model
"""
def detect_anomalies(device_id, raw_data):
    # print("anomaly detection function", flush=True)
    model, scaler, threshold = get_anomaly_detection_model(device_id)

    # Ensure raw_data isn't empty
    if not raw_data:
        raise ValueError("Raw data is empty. Cannot perform anomaly detection.")

    df_window = pd.DataFrame(raw_data)
    df_window['timestamp'] = pd.to_datetime(df_window['timestamp'], unit='s')
    df_window = df_window.sort_values('timestamp').reset_index(drop=True)

    features = extract_window_features(df_window)
    X_input = pd.DataFrame([features])
    X_scaled = scaler.transform(X_input)

    # print("calling anomaly detection model...", flush=True)
    
    X_pred = model.predict(X_scaled)
    # print("call done, reconstructing...", flush=True)
    recon_error = np.mean((X_scaled - X_pred) ** 2, axis=1)[0]
    # print("reconstruction done, determining is it anomaly?", flush=True)
    is_anomaly = int(recon_error > threshold)

    if is_anomaly:
        # print("there are anomalies", flush=True)
        # Find the most anomalous feature
        anomaly_feature_idx = np.argmax(np.abs(X_scaled - X_pred), axis=1)[0]

        # Ensure the anomaly feature index is within bounds
        if anomaly_feature_idx < 0 or anomaly_feature_idx >= len(df_window):
            raise ValueError(f"Anomaly feature index {anomaly_feature_idx} is out of bounds.")

        feature_names = X_input.columns.tolist()
        most_anomalous_feature = feature_names[anomaly_feature_idx]

        timestamp_start = df_window["timestamp"].iloc[0].isoformat()
        timestamp_end = df_window["timestamp"].iloc[-1].isoformat()
        most_anomalous_feature_timestamp = df_window['timestamp'].iloc[anomaly_feature_idx].isoformat()
        
        return {
            "timestamp_start": timestamp_start,
            "timestamp_end": timestamp_end,
            "reconstruction_error": recon_error,
            "most_anomalous_feature": most_anomalous_feature,
            "message": f"Anomaly in '{most_anomalous_feature}' at {most_anomalous_feature_timestamp}"
        }
    
    # else:
    #     print("NO ANOMALIES", flush=True)

"""
Generate anomaly message based on type
"""
def generate_message():
    return None
