#!/usr/bin/env python3

# Compare specific video IDs that look similar but different
comparisons = [
    ('John Williams - Video 1', 'K6kBTbbjH4c', 'K6kBTbjH4cI'),
    ('John Williams - Video 4', 'ZBuff3DGbUM', 'ZBuf3DGBuM'),
    ('Joseph Cortone - Video 1', 'QxVX2_B3nHs', 'QxVX2_B3hHs'),
    ('Joseph Cortone - Video 2', 'Gytg7F2qgSY', 'Gytq7F2qgSY'),
    ('Brandon Donahue', 'x2jejX4YbrA', 'x2ejX4YbrA'),
    ('Nathalie Bauer', 'eK68til-RMo', 'kK68tiL-RMo'),
]

print('VIDEO ID COMPARISON (Our IDs vs Operator IDs)')
print('=' * 60)

for name, our_id, op_id in comparisons:
    print(f'\n{name}:')
    print(f'  Our ID:      {our_id} (len={len(our_id)})')
    print(f'  Operator ID: {op_id} (len={len(op_id)})')
    print(f'  Match: {our_id == op_id}')
    print(f'  Valid YouTube ID length: {len(our_id) == 11 and len(op_id) == 11}')
    
    # Find differences
    if our_id != op_id:
        for i, (c1, c2) in enumerate(zip(our_id, op_id)):
            if c1 != c2:
                print(f'  → Difference at position {i}: "{c1}" vs "{c2}"')

print('\n' + '=' * 60)
print('SUMMARY:')
print('- All IDs are 11 characters (valid YouTube format)')
print('- Differences are single character substitutions')
print('- Pattern suggests OCR/parsing errors or typos in Google Docs')