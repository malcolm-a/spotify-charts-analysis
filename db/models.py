from sqlalchemy import Column, String, ForeignKey, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint  # Add this import

Base = declarative_base()
metadata = MetaData()

artist_song = Table(
    'artist_song', 
    Base.metadata,
    Column('artist_id', String, ForeignKey('artist.spotify_id'), primary_key=True),
    Column('song_id', String, ForeignKey('song.song_id'), primary_key=True),
    UniqueConstraint('artist_id', 'song_id', name='uq_artist_song')
)

class Artist(Base):
    __tablename__ = 'artist'
    
    spotify_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    
    __table_args__ = (UniqueConstraint('spotify_id', name='uq_artist_id'),)
    
    def __repr__(self):
        return f"<Artist(name='{self.name}', spotify_id='{self.spotify_id}')>"

class Song(Base):
    __tablename__ = 'song'
    
    song_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    
    __table_args__ = (UniqueConstraint('song_id', name='uq_song_id'),)
    
    def __repr__(self):
        return f"<Song(name='{self.name}', song_id='{self.song_id}')>"
