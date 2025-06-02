from pydantic import BaseModel

class PowerTelemetry(BaseModel):
    timestamp: str
    voltage: float
    current: float
    pf: float

    is_on: bool

    power: float
    energy: float
    frequency: float
