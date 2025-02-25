from pydantic import BaseModel
from datetime import datetime
from typing import Optional

# Schema para envio de telemetria
class TelemetriaCreate(BaseModel):
    cust_id: int
    timestamp: datetime
    speed: float
    fuel: float
    lap_time: float
    sector1: float
    sector2: float
    sector3: float

# Schema para resposta (inclui ID)
class TelemetriaResponse(TelemetriaCreate):
    id: int
