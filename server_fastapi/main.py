from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import models
import database
import schemas

# Criar tabelas no banco
models.Base.metadata.create_all(bind=database.engine)

# Inicializar FastAPI
app = FastAPI()

# Endpoint para salvar telemetria
@app.post("/telemetria/", response_model=schemas.TelemetriaResponse)
def salvar_telemetria(data: schemas.TelemetriaCreate, db: Session = Depends(database.get_db)):
    nova_telemetria = models.Telemetria(**data.dict())
    db.add(nova_telemetria)
    db.commit()
    db.refresh(nova_telemetria)
    return nova_telemetria

# Endpoint para obter telemetria de um piloto
@app.get("/telemetria/{cust_id}", response_model=list[schemas.TelemetriaResponse])
def obter_telemetria(cust_id: int, db: Session = Depends(database.get_db)):
    return db.query(models.Telemetria).filter(models.Telemetria.cust_id == cust_id).all()

