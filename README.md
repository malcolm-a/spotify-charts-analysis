# Spotify Charts Analysis - ETL Pipeline

A **Data Engineering ETL pipeline** that collects, processes, and analyzes music streaming data from multiple sources. It is aimed at providing all the data needed for analysis and visualization of music trends and preferences per market.

## 👷🏾‍♂️ Project Architecture

This is a project that implements:
- **Extract**: Data collection from Spotify API, kworb.net charts, and streaming statistics
- **Transform**: Data cleaning, normalization, and enrichment
- **Load**: Bulk operations into PostgreSQL with upsert capabilities
- **Orchestration**: Automated pipeline scheduling


## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/spotify-charts-analysis
cd spotify-charts-analysis
```

### 2. Set Up Environment Variables

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=spotify_charts
DB_USER=postgres
DB_PASSWORD=postgres

# Spotify API Credentials (get from https://developer.spotify.com/dashboard/)
SPOTIFY_CLIENT_ID=your_actual_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_actual_spotify_client_secret
```

### 3. Install Dependencies

```bash
pip install -r config/requirements.txt
```

### 4. Launch Infrastructure

```bash
cd docker
docker-compose up -d
```

### 5. Initialize Database

```bash
python scripts/setup_database.py
```

## 👨🏾‍💻 Usage

### Run Complete ETL Pipeline

```bash
# Run all pipelines once
python scripts/run_etl_pipeline.py

# Run with continuous scheduling
python scripts/run_etl_pipeline.py scheduler
```

### Run Individual Pipelines

```bash
# Daily charts only
python -m src.pipelines.orchestrator --mode charts

# Artist stats only  
python -m src.pipelines.orchestrator --mode stats

# Spotify metadata only
python -m src.pipelines.orchestrator --mode metadata
```

### Test Components

```bash
# Run test suite with pytest
pytest scripts/test_etl.py -v
```


## 🌐 Data Sources

- **Spotify Web API**: Artist metadata, track features, audio analysis
- **kworb.net**: Daily charts, streaming statistics, listener counts
- **MusicBrainz**: Artist identification and metadata enrichment (unused)

