from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, create_engine, Session

engine = None
SessionLocal = None

def init_db(database_url):
    global engine
    global SessionLocal
    if engine is not None:
        # return engine
        raise RuntimeError("engine already initiated.")
    
    engine = create_engine(database_url, echo=False)
    # SQLModel.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, class_=Session)
    return engine

def get_session():
    # with Session(engine) as session:
        # yield session
    return SessionLocal()
