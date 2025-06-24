# Music-Viz

Music-Viz is a data project designed to collect, store, and analyze music streaming data from various online sources. It fetches daily information about artists, songs, and charts from Spotify and Kworb.net, saving the data into a PostgreSQL database for further analysis and visualization with Metabase.

## Features

- **Automated Data Collection**: Scripts to automatically fetch daily Spotify charts, artist statistics, track details, and more.
- **Robust Database Schema**: Utilizes a PostgreSQL database with a schema managed by SQLAlchemy and Alembic for migrations.
- **Dockerized Environment**: Comes with a `docker-compose.yml` for easy setup of a development environment, including PostgreSQL, Metabase for business intelligence, and Adminer/pgAdmin for database management.
- **Scheduled Jobs**: Scripts are designed to be run on a schedule to ensure the data stays current.

## Getting Started

Follow these steps to get the project up and running on your local machine.

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

### 1. Clone the Repository

```sh
git clone https://github.com/malcolm-a/music-viz
cd music-viz
```

### 2. Set Up Environment Variables

You'll need to provide credentials for the database and the Spotify API. Create a `.env` file in the project root:

```env
# .env file
# PostgreSQL Connection
DB_HOST=db
DB_PORT=5432
DB_NAME=
DB_USER=
DB_PASSWORD=

# Spotify API Credentials
SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=
```
**Note**: The default database credentials in the `.env` file match the ones in `docker-compose.yml`.

### 3. Install Dependencies

Install the required Python packages using pip:

```sh
pip install -r requirements.txt
```

### 4. Launch Services with Docker

Start the PostgreSQL database and other services using Docker Compose:

```sh
docker-compose up -d
```

### 5. Apply Database Migrations

Once the database container is running, apply the latest schema migrations using Alembic:

```sh
alembic upgrade head
```

## Usage

The data collection scripts are located in the `scripts/` directory. You can run them individually to populate your database.

To run all the scripts in a sequence for a full data refresh, you can use the `run_dataviz.py` script, which is also designed to be run as a service.

## Database Schema

The main database tables are:

- `artist`: Stores artists' information, including their Spotify ID and name.
- `song`: Stores songs' information, including their song ID and name.
- `artist_song`: A many-to-many association table linking artists and songs.
- `country`: A list of countries used for region-specific charts.
- `spotify_charts`: Contains daily song chart data, including rank and stream counts for each country.
- `artist_stats`: Stores daily statistics for artists, such as total streams and listener counts.