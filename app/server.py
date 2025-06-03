from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime, timezone
from app.modules.forecast import generate_forecast
# from app.modules.persist import get_persist as get_persisted_telemetry
from app.config.forecast import ALLOWED_HORIZONS, SEQUENCE_LENGTH
from app.storage.telemetry_buffer import get_latest_buffer
import traceback

app = FastAPI(title="CermatListrik Inference Server", version="1.0")

allowed_horizons = ", ".join(map(str, ALLOWED_HORIZONS))

@app.get("/forecast/{device_id}")
def get_forecasted_power(
    device_id: str,
    horizon: int = Query(
        600,
        description=f"Forecast horizon in seconds. Only accepts {allowed_horizons}."
    ),
):
    if horizon not in ALLOWED_HORIZONS:
        raise HTTPException(status_code=400, detail=f"Invalid horizon. Must be one of {allowed_horizons}.")

    try:
        # Get the latest buffered data for the past window length
        history_data = get_latest_buffer(device_id, seconds_prior=SEQUENCE_LENGTH)

        if not history_data or len(history_data) < SEQUENCE_LENGTH:
            raise HTTPException(status_code=400, detail="Waiting for buffered data")
            # history_data = get_latest_persisted_telemetry(device_id, starts_at)

        forecast = generate_forecast(device_id, history_data, horizon)
        return { "forecast": forecast }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

@app.get("/telemetry/power-consumption/{device_id}/latest")
def get_latest_power_consumption_by_device_id(
    device_id: str,
):
    try:
        latest_data = get_latest_buffer(device_id, seconds_prior=600)  # ~10 minutes if 1s frequency

        if not latest_data:
            raise HTTPException(status_code=404, detail="No recent power telemetry found.")

        formatted = [
            {
                # "timestamp": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc).isoformat(),
                "timestamp": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc).replace(tzinfo=None).isoformat(),
                "power": float(item.get("power", 0.0))
            }
            for item in latest_data
            if "timestamp" in item and "power" in item
        ]

        return { "latest_power": formatted }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve telemetry: {str(e)}")
