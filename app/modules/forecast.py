import numpy as np
import pandas as pd
from datetime import timedelta
from app.utils.model_loader import get_forecasting_model
from app.config.forecast import SEQUENCE_LENGTH

features = ['voltage', 'current', 'pf', 'minute_sin', 'minute_cos', 'hour_sin', 'hour_cos']

def generate_forecast(device_id, history_data, horizon):
    history = pd.DataFrame(history_data)

    # Time features
    history['timestamp'] = pd.to_datetime(history['timestamp'], unit='s')
    history['minute'] = history['timestamp'].dt.minute
    history['hour'] = history['timestamp'].dt.hour
    history['minute_sin'] = np.sin(2 * np.pi * history['minute'] / 60)
    history['minute_cos'] = np.cos(2 * np.pi * history['minute'] / 60)
    history['hour_sin'] = np.sin(2 * np.pi * history['hour'] / 24)
    history['hour_cos'] = np.cos(2 * np.pi * history['hour'] / 24)

    # Select & scale
    X = history[features].values
    if len(X) < SEQUENCE_LENGTH:
        raise ValueError(f"Insufficient history: need at least {SEQUENCE_LENGTH} records")

    # Predict
    model, scaler_X, scaler_y = get_forecasting_model(device_id, horizon)
    
    X_seq = X[-SEQUENCE_LENGTH:]
    X_scaled = scaler_X.transform(X_seq)
    X_scaled = np.expand_dims(X_scaled, axis=0)

    y_pred_scaled = model.predict(X_scaled)
    y_pred = scaler_y.inverse_transform(y_pred_scaled).flatten().tolist()

    # Future timestamps
    last_time = history['timestamp'].iloc[-1]
    future_timestamps = [(last_time + timedelta(seconds=i+1)).isoformat() for i in range(horizon)]

    return [{"timestamp": ts, "power": p} for ts, p in zip(future_timestamps, y_pred)]
