# S3 Upload Summary

## Upload Results
- **Total files uploaded**: 69 files
- **Total size**: ~13+ GB
- **S3 Bucket**: `typing-clients-storage-2025`
- **Region**: us-east-1

## Organization Structure
Files are organized in S3 with the following structure:
```
{row_id}/{person_name}/{filename}
```

Example:
- `489/Dan_Jane/drive_file_1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd` (6.2GB video)
- `487/Olivia_Tomlinson/youtube_8zo0I4-F3Bs.mp3`

## CSV Updates
The `outputs/output.csv` file has been updated with three new columns:
1. **s3_youtube_urls**: Pipe-delimited list of S3 URLs for YouTube content
2. **s3_drive_urls**: Pipe-delimited list of S3 URLs for Drive content  
3. **s3_all_files**: Pipe-delimited list of all S3 URLs for the person

## Access URLs
All files are publicly accessible via URLs in the format:
```
https://typing-clients-storage-2025.s3.amazonaws.com/{row_id}/{person_name}/{filename}
```

## Complete Upload List
- ✅ All 15 people with assets have their files uploaded
- ✅ 22+ YouTube videos (various formats: MP3, MP4, M4A)
- ✅ 5 large Google Drive files (total ~13GB)
- ✅ Multiple info JSON files for Drive folders and playlists
- ✅ CSV properly updated with S3 URLs for each row

## Notable Large Files
1. Dan Jane (489): 6.2GB Drive video
2. Brenden Ohlsson (474): 4.7GB Drive file
3. Caroline Chiu (497): 1.3GB Drive file
4. Shelsea Evans (486): 990MB Drive file
5. Kiko (492): 866MB Drive file