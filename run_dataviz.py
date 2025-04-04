#!/usr/bin/env python3
import time
import subprocess
from datetime import datetime

SCRIPTS = [
    "/home/malcolm/music_dataviz_prod/sae_datavis/scripts/spotify_charts.py",
    "/home/malcolm/music_dataviz_prod/sae_datavis/scripts/fetch_artists_stats.py",
    "/home/malcolm/music_dataviz_prod/sae_datavis/scripts/fetch_sp_artist.py",
    "/home/malcolm/music_dataviz_prod/sae_datavis/scripts/fetch_sp_track.py"
]

def run_scripts():
    print(f"{datetime.now()} Starting scripts...")
    for script in SCRIPTS:
        try:
            subprocess.run(["python3", script], check=True)
            print(f"Completed {script}")
        except subprocess.CalledProcessError as e:
            print(f"Error running {script}: {e}")

if __name__ == "__main__":
    while True:
        run_scripts()
        time.sleep(86400 - time.time() % 86400)
