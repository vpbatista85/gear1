from sqlalchemy import Column, Integer, Float, String, DateTime
from database import Base
from datetime import datetime

class Telemetria(Base):
    __tablename__ = "telemetria"

    id = Column(Integer, primary_key=True, index=True)
    cust_id = Column(Integer, index=True)  # ID do piloto
    timestamp = Column(DateTime, default=datetime.utcnow)  # Horário da telemetria
    speed = Column(Float)  # Velocidade
    fuel = Column(Float)  # Combustível restante
    lap_time = Column(Float)  # Tempo da volta
    sector1 = Column(Float)  # Setor 1
    sector2 = Column(Float)  # Setor 2
    sector3 = Column(Float)  # Setor 3
