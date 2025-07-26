import pandas as pd

# Load CSV and check new people's links
df = pd.read_csv('outputs/output.csv')
new_people = df[df['row_id'] > 496]

print('New people with links:')
print('=' * 80)

for _, person in new_people.iterrows():
    youtube = str(person.get('youtube_playlist', ''))
    drive = str(person.get('google_drive', ''))
    
    has_youtube = youtube and youtube \!= 'nan' and youtube.strip()
    has_drive = drive and drive \!= 'nan' and drive.strip()
    
    if has_youtube or has_drive:
        print(f'\nRow {person["row_id"]}: {person["name"]}')
        if has_youtube:
            print(f'  YouTube: {youtube[:100]}...' if len(youtube) > 100 else f'  YouTube: {youtube}')
        if has_drive:
            print(f'  Drive: {drive[:100]}...' if len(drive) > 100 else f'  Drive: {drive}')
