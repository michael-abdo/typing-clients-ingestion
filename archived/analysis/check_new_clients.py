import pandas as pd

# Load CSV
df = pd.read_csv('outputs/output.csv')

# Check for the new clients
client_ids = [503, 504, 505, 506]
clients = df[df['row_id'].isin(client_ids)]

if clients.empty:
    print('New clients not found in CSV yet')
    print(f'Max row_id still: {df["row_id"].max()}')
    print(f'Total rows: {len(df)}')
else:
    print('New clients found in CSV:')
    print('=' * 80)
    for _, client in clients.iterrows():
        row_id = client['row_id']
        name = client['name']
        email = client.get('email', '')
        doc_text = str(client.get('document_text', ''))
        youtube_links = str(client.get('youtube_playlist', ''))
        
        print(f'\nRow {row_id}: {name}')
        print(f'Email: {email}')
        has_doc = doc_text and doc_text \!= 'nan'
        print(f'Document extracted: {"Yes" if has_doc else "No"}')
        if has_doc:
            print(f'Document text length: {len(doc_text)} chars')
        if youtube_links and youtube_links \!= 'nan':
            print(f'YouTube links: {youtube_links}')
