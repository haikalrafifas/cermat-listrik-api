from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
from app.modules.forecast import generate_forecast
# from app.modules.persist import get_persist as get_persisted_telemetry
from app.config.forecast import ALLOWED_HORIZONS, SEQUENCE_LENGTH
from app.storage.telemetry_buffer import get_buffer as get_buffered_telemetry

app = FastAPI(title="CermatListrik Inference Server", version="1.0")

allowed_horizons = ", ".join(map(str, ALLOWED_HORIZONS))
@app.get("/forecast/{device_id}")
def get_forecasted_power(
    device_id: str,
    starts_at: datetime = Query(..., description="The starting point for the forecasting."),
    horizon: int = Query(
        600,
        description=f"Forecast horizon in seconds. Only accepts {allowed_horizons}."
    ),
):
    if horizon not in ALLOWED_HORIZONS:
        raise HTTPException(status_code=400, detail=f"Invalid horizon. Must be one of {allowed_horizons}.")

    try:
        history_data = get_buffered_telemetry(device_id, starts_at)
        if not history_data or len(history_data) < SEQUENCE_LENGTH:
            raise HTTPException(status_code=400, detail="Waiting for buffered data")
            # history_data = get_persisted_telemetry(device_id, starts_at)

        forecast = generate_forecast(device_id, history_data, horizon)
        return { "forecast": forecast }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
