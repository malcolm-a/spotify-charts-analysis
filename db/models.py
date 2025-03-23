from sqlalchemy import Column, String, Integer, ForeignKey, Table, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

# Association table for many-to-many relationship
artist_song = Table(
    'artist_song',
    Base.metadata,
    Column('artist_id', String, ForeignKey('artist.spotify_id'), nullable=False),
    Column('song_id', String, ForeignKey('song.song_id'), nullable=False),
)

class Artist(Base):
    __tablename__ = 'artist'
    
    spotify_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    
    songs = relationship('Song', secondary=artist_song, back_populates='artists')
    
    def __repr__(self):
        return f"<Artist(spotify_id='{self.spotify_id}', name='{self.name}')>"

class Song(Base):
    __tablename__ = 'song'
    
    song_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    sp_track = Column(JSONB, nullable=True)
    features = Column(JSONB, nullable=True)
    
    artists = relationship('Artist', secondary=artist_song, back_populates='songs')
    
    def __repr__(self):
        return f"<Song(song_id='{self.song_id}', name='{self.name}')>"