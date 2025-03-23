import subprocess
import re
import dotenv
import os
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd

dotenv.load_dotenv()

DB_NAME = os.getenv("DB_NAME", "your_database_name")
PSQL_COMMAND = ["psql", "-d", DB_NAME, "-c"]
RUNS = 100

RELATIONAL_QUERY = """
EXPLAIN ANALYZE
SELECT 
    a1.name AS artist1_name,
    a2.name AS artist2_name,
    COUNT(*) AS collaboration_count
FROM benchmark.artist_song ars1
JOIN benchmark.artist_song ars2 ON ars1.song_id = ars2.song_id
JOIN benchmark.artist a1 ON ars1.artist_id = a1.spotify_id
JOIN benchmark.artist a2 ON ars2.artist_id = a2.spotify_id
WHERE a1.spotify_id < a2.spotify_id  
GROUP BY a1.name, a2.name
ORDER BY collaboration_count DESC;
"""

JSONB_QUERY = """
EXPLAIN ANALYZE
WITH collaborations AS (
    SELECT
        artist1->>'id' AS artist1_id,
        artist1->>'name' AS artist1_name,
        artist2->>'id' AS artist2_id,
        artist2->>'name' AS artist2_name,
        COUNT(*) AS collaboration_count
    FROM benchmark.song,
    LATERAL jsonb_array_elements(artist_ids->'artists') AS artist1,
    LATERAL jsonb_array_elements(artist_ids->'artists') AS artist2
    WHERE artist1->>'id' < artist2->>'id'
    GROUP BY artist1->>'id', artist1->>'name', artist2->>'id', artist2->>'name'
)
SELECT
    artist1_name,
    artist2_name,
    collaboration_count
FROM collaborations
ORDER BY collaboration_count DESC;
"""

def run_query(query, query_name):
    """Run benchmark query, print and return execution times

    Args:
        query (_type_): the query to run
        query_name (_type_): how to name the query in the output

    Returns:
        list: list of execution times
    """    
    
    print(f"Benchmarking {query_name}...")
    total_time = 0
    execution_times = []

    for i in range(1, RUNS + 1):
        result = subprocess.run(
            PSQL_COMMAND + [query],
            capture_output=True,
            text=True
        )
        match = re.search(r"Execution Time: ([\d.]+) ms", result.stdout)
        execution_time = float(match.group(1))
        execution_times.append(execution_time)
        total_time += execution_time

    average_time = total_time / RUNS
    print(f"Average {query_name} Time: {average_time:.2f} ms")
    print(f"Execution Times: {execution_times}\n")
    return execution_times


data = {
    "Run": np.arange(1, RUNS + 1),
    "Relational Query": run_query(RELATIONAL_QUERY, "Relational Query"),
    "JSONB Query": run_query(JSONB_QUERY, "JSONB Query")
}

# Plot
df = pd.DataFrame(data)
df_melted = df.melt(id_vars=["Run"], var_name="Query Type", value_name="Execution Time")

plt.figure(figsize=(12, 6))
sns.lineplot(data=df_melted, x="Run", y="Execution Time", hue="Query Type", marker="o")
plt.title("Benchmarking Relational vs JSONB Queries")
plt.xlabel("Run")
plt.ylabel("Execution Time (ms)")
plt.legend(title="Query Type")
plt.grid(True)
plt.tight_layout()
plt.show()