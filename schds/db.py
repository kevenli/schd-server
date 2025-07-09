from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, create_engine, Session
from alembic import context
from schds.models import create_tables

engine = None
SessionLocal = None

def init_db(database_url, auto_upgrade=True):
    global engine
    global SessionLocal
    if engine is not None:
        # return engine
        raise RuntimeError("engine already initiated.")
    
    engine = create_engine(database_url, echo=False)
    # SQLModel.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, class_=Session)
    # if database_url.startswith('sqlite'):
    #     # sqlite database does not support scheme migration, create directly.
    #     create_tables(engine)
    # else:
    #     upgrade_database(database_url)
    if auto_upgrade:
        upgrade_database(database_url)
        
    return engine

def get_session():
    # with Session(engine) as session:
        # yield session
    return SessionLocal()


def upgrade_database(database_url):
    from alembic.config import Config
    from alembic import command
    alembic_cfg = Config()

    migrations_path = 'schds:migrations'
    alembic_cfg.set_main_option('script_location', migrations_path)
    alembic_cfg.set_main_option('sqlalchemy.url', database_url)
    command.upgrade(alembic_cfg, 'head')
