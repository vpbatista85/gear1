from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Configurações do Banco (Railway fornece essas credenciais)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@host:port/database")

# Criar engine e sessão do SQLAlchemy
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para os modelos
Base = declarative_base()

# Função para pegar sessão do DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
