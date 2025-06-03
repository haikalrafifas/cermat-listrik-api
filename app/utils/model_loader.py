import joblib
from tensorflow.keras.models import load_model
from app.utils.filesystem import with_current_dir
from app.storage.model_cache import _model_cache
from app.config.forecast import THRESHOLD

def with_model_dir(model):
    return with_current_dir("../", "models", model)

"""
Loads forecasting model and its components
"""
def get_forecasting_model(device_id, horizon):
    namespace = f"{device_id}/forecast"
    tag = f"{namespace}.{horizon}s"

    if tag not in _model_cache:
        _model_cache[tag] = {
            "model": load_model(with_model_dir(f"{tag}.keras")),
            "scaler_X": joblib.load(with_model_dir(f"{namespace}.scaler_x.pkl")),
            "scaler_y": joblib.load(with_model_dir(f"{namespace}.scaler_y.pkl")),
        }

    cached = _model_cache[tag]
    return cached["model"], cached["scaler_X"], cached["scaler_y"]

"""
Loads anomaly detection model and its components
"""
def get_anomaly_detection_model(device_id):
    namespace = f"{device_id}/anomaly-detection"

    if namespace not in _model_cache:
        _model_cache[namespace] = {
            "model": load_model(with_model_dir(f"{namespace}.keras")),
            "scaler": joblib.load(with_model_dir(f"{namespace}.scaler.pkl")),
            "threshold": THRESHOLD,
        }

    cached = _model_cache[namespace]
    return cached["model"], cached["scaler"], cached["threshold"]
