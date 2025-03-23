import os
from dotenv import load_dotenv
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

load_dotenv()

db_config = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "database": os.environ.get("DB_NAME", "music_dataviz"),
    "user": os.environ.get("DB_USER", "malcolm"),
    "password": os.environ.get("DB_PASSWORD", "malcolm"),
    "connect_timeout": 60,
    "application_name": "music_dataviz",
}


connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

engine = sa.create_engine(
    connection_string,
    pool_size=10,
    max_overflow=20,
    pool_timeout=60,
    pool_recycle=1800,
    pool_pre_ping=True 
)

def get_engine():
    return engine

def get_session():
    Session = sessionmaker(bind=engine)
    return Session()