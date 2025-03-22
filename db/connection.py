import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

db_config = {
    "host": "10.31.33.186", 
    "port": 5433,  # Changed to match Docker's exposed port
    "database": "postgres",  
    "user": "postgres", 
    "password": "postgres",
    "connect_timeout": 5,
    "application_name": "docker_postgres"  # Help identify the connection
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