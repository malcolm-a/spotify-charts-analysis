from sqlalchemy import Column, PrimaryKeyConstraint, String, Integer, ForeignKey, Table, Date, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint


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
    sp_artist = Column(JSONB, nullable=True)
    mbid = Column(String, nullable=True)
    
    songs = relationship('Song', secondary=artist_song, back_populates='artists')
    
    def __repr__(self):
        return f"<Artist(spotify_id='{self.spotify_id}', name='{self.name}')>"

class Song(Base):
    __tablename__ = 'song'
    
    song_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    sp_track = Column(JSONB, nullable=True)
    features = Column(JSONB, nullable=True)
    mbid = Column(String, nullable=True)
    
    artists = relationship('Artist', secondary=artist_song, back_populates='songs')
    
    def __repr__(self):
        return f"<Song(song_id='{self.song_id}', name='{self.name}')>"
    
class Artist_stats(Base):
    __tablename__ = "artist_stats"

    artist_id = Column(String, ForeignKey('artist.spotify_id'), nullable=False)
    date = Column(Date, nullable=False)
    total_streams = Column(BigInteger)
    daily_streams = Column(Integer)
    listeners = Column(BigInteger)
    
    

    __table_args__ = (
        UniqueConstraint('artist_id', 'date', name='uq_artists_stats'),
        PrimaryKeyConstraint('artist_id', 'date'),
    )

    def __repr__(self):
        return f"<ArtistStats(artist='{self.artist_name}', date='{self.date}', streams='{self.total_streams}')>"
    
class Country(Base):
    __tablename__ = 'country'
    
    country_code = Column(String, primary_key=True)
    country_name = Column(String, nullable=False)
    region = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<Country(country_code='{self.country_code}', country_name='{self.country_name}')>"
    
class Spotify_charts(Base):
    __tablename__ = 'spotify_charts'
    
    date = Column(Date, nullable=False)
    country_code = Column(String, ForeignKey('country.country_code'), nullable=False)
    song_id = Column(String, ForeignKey('song.song_id'), nullable=False)

    streams = Column(BigInteger, nullable=False)
    total_streams = Column(BigInteger, nullable=False)
    days = Column(Integer, nullable=False)
    rank = Column(Integer, nullable=False)

    
    __table_args__ = (
        UniqueConstraint('song_id', 'country_code', 'date', name='uq_spotify_charts'),
        PrimaryKeyConstraint('song_id', 'country_code', 'date'),
    )
        
    def __repr__(self):
        return f"<SpotifyCharts(song_id='{self.song_id}', country_code='{self.country_code}', rank='{self.rank}')>"