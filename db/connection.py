import sqlalchemy as sa

db_config = {
    "host": "localhost", 
    "port": 5432, 
    "database": "postgres",  
    "user": "postgres", 
    "password": "postgres"  
}

connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = sa.create_engine(connection_string) 

def get_engine():
    return engine