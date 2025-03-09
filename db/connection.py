import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

db_config = {
    "host": "localhost", 
    "port": 5433,  # Changed to match Docker's exposed port
    "database": "postgres",  
    "user": "postgres", 
    "password": "postgres",
    "connect_timeout": 5,
    "application_name": "docker_postgres"  # Help identify the connection
}

connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = sa.create_engine(connection_string)

def get_engine():
    return engine

def get_session():
    Session = sessionmaker(bind=engine)
    return Session()