import pandas as pd
import networkx as nx
import plotly.graph_objects as go
from db.connection import get_engine

# Connexion à la base de données
engine = get_engine()

# Requête SQL pour obtenir les collaborations
# Nous avons besoin de savoir quels artistes ont travaillé sur les mêmes chansons
query = """
SELECT s.song_id, s.name as song_name, a.spotify_id as artist_id, a.name as artist_name
FROM song s
JOIN artist a ON s.artist_id = a.spotify_id
ORDER BY s.song_id, a.name
"""

# Charger les données dans un DataFrame
df = pd.read_sql(query, engine)

# Créer un DataFrame de collaborations
# Pour chaque chanson avec plusieurs artistes, créer toutes les paires possibles
collaborations = []

# Regrouper par chanson
songs = df.groupby('song_id')

for song_id, group in songs:
    artists = group[['artist_id', 'artist_name']].drop_duplicates().values.tolist()
    
    # S'il y a plus d'un artiste sur la chanson, c'est une collaboration
    if len(artists) > 1:
        for i in range(len(artists)):
            for j in range(i+1, len(artists)):
                artist1_id, artist1_name = artists[i]
                artist2_id, artist2_name = artists[j]
                song_name = group['song_name'].iloc[0]
                
                collaborations.append({
                    'artist1_id': artist1_id,
                    'artist1_name': artist1_name,
                    'artist2_id': artist2_id,
                    'artist2_name': artist2_name,
                    'song_id': song_id,
                    'song_name': song_name
                })

collab_df = pd.DataFrame(collaborations)

# Compter le nombre de collaborations entre chaque paire d'artistes
if not collab_df.empty:
    collab_count = collab_df.groupby(['artist1_id', 'artist1_name', 'artist2_id', 'artist2_name']).size().reset_index(name='collab_count')
    
    # Créer un graphe non-dirigé
    G = nx.Graph()
    
    # Ajouter tous les artistes comme nœuds
    all_artists = pd.concat([
        df[['artist_id', 'artist_name']].rename(columns={'artist_id': 'id', 'artist_name': 'name'}),
    ]).drop_duplicates()
    
    for _, artist in all_artists.iterrows():
        G.add_node(artist['id'], name=artist['name'])
    
    # Ajouter les arêtes pour les collaborations
    for _, row in collab_count.iterrows():
        G.add_edge(
            row['artist1_id'], 
            row['artist2_id'], 
            weight=row['collab_count'],
            title=f"{row['collab_count']} collaboration(s)"
        )
    
    # Calculer la centralité des artistes (nombre de connexions)
    centrality = nx.degree_centrality(G)
    for node in G.nodes():
        G.nodes[node]['centrality'] = centrality[node]
    
    # Disposition du graphe avec l'algorithme Fruchterman-Reingold
    pos = nx.spring_layout(G, seed=42)
    
    # Créer le tracé pour Plotly
    edge_trace = []
    
    # Tracer les arêtes
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        weight = edge[2]['weight']
        
        edge_trace.append(
            go.Scatter(
                x=[x0, x1, None], 
                y=[y0, y1, None],
                line=dict(width=weight, color='rgba(150,150,150,0.7)'),
                hoverinfo='none',
                mode='lines'
            )
        )
    
    # Tracer les nœuds
    node_x = []
    node_y = []
    node_text = []
    node_size = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        name = G.nodes[node]['name']
        centrality = G.nodes[node]['centrality']
        connections = G.degree[node]
        node_text.append(f"{name}<br>Collaborations: {connections}")
        node_size.append(centrality * 100 + 15)  # Taille basée sur la centralité
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=node_size,
            color=[G.degree[node] for node in G.nodes()],
            colorbar=dict(
                title='Nombre de collaborations',
                thickness=15,
            ),
            line=dict(width=2)
        )
    )
    
    # Créer la figure
    fig = go.Figure(data=edge_trace + [node_trace],
                    layout=go.Layout(
                        title='Réseau de collaborations entre artistes',
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                    ))
    
    # Afficher le graphique
    fig.show()
    
    # Pour sauvegarder l'image
    fig.write_html("data/html/collaborations_network.html")
    
else:
    print("Aucune collaboration trouvée dans les données.")