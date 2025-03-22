from db.connection import get_engine
from db.models import Base
from sqlalchemy import inspect, text

def drop_and_create_schema():
    """Drop existing tables and recreate the schema"""
    engine = get_engine()
    inspector = inspect(engine)
    
    # Drop tables if they exist
    if 'artist_song' in inspector.get_table_names():
        print("Dropping artist_song table...")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE artist_song CASCADE"))
            conn.commit()
            
    if 'artist' in inspector.get_table_names():
        print("Dropping artist table...")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE artist CASCADE"))
            conn.commit()
    
    if 'song' in inspector.get_table_names():
        print("Dropping song table...")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE song CASCADE"))
            conn.commit()
    
    # Create all tables
    print("Creating new schema...")
    Base.metadata.create_all(engine)
    print("Schema created successfully!")

def ensure_schema_exists():
    """Check if schema exists and create it if not"""
    engine = get_engine()
    inspector = inspect(engine)
    
    # Check if tables exist
    tables_exist = all(table in inspector.get_table_names() 
                      for table in ['artist', 'song', 'artist_song'])
    
    if not tables_exist:
        print("Some tables are missing, creating schema...")
        Base.metadata.create_all(engine)
        print("Schema created successfully!")

if __name__ == "__main__":
    drop_and_create_schema()
