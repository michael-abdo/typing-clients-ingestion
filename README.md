# Personality Typing Content Manager

A production-ready system for downloading and tracking personality typing videos, documents, and transcripts from Google Sheets sources.

## 🚀 Features

- **Automated Content Collection**: Downloads YouTube videos/transcripts and Google Drive files
- **Row-Centric Tracking**: Maintains perfect CSV row relationships throughout the download pipeline
- **Data Integrity**: Preserves personality type data with atomic CSV updates
- **Bidirectional Mapping**: Links downloaded files back to source CSV rows
- **Error Resilience**: Intelligent retry system with permanent failure detection
- **Production Monitoring**: Real-time health checks and alerts

## 📋 Prerequisites

- Python 3.8+
- yt-dlp (for YouTube downloads)
- Required Python packages in `requirements.txt`

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/ops-typing-log-client.git
cd ops-typing-log-client
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure settings:
```bash
cp config/config.yaml.example config/config.yaml
# Edit config/config.yaml with your settings
```

## 🎯 Quick Start

### Basic Usage

Run the complete workflow:
```bash
python run_complete_workflow.py
```

### Advanced Options

```bash
# Skip Google Sheets scraping (use existing CSV)
python run_complete_workflow.py --skip-sheet

# Limit downloads for testing
python run_complete_workflow.py --max-youtube 10 --max-drive 5

# Use existing CSV links (fastest)
python run_complete_workflow.py --use-csv-links
```

### Monitoring

Check system status:
```bash
python utils/monitoring.py --status
```

View download statistics:
```bash
python utils/csv_tracker.py --status
```

## 📁 Project Structure

```
.
├── run_complete_workflow.py    # Main entry point
├── utils/                      # Core utilities
│   ├── download_youtube.py     # YouTube downloader
│   ├── download_drive.py       # Google Drive downloader
│   ├── csv_tracker.py          # CSV tracking system
│   ├── monitoring.py           # System monitoring
│   └── ...                     # Other utilities
├── config/                     # Configuration files
├── outputs/                    # CSV output files
├── youtube_downloads/          # Downloaded videos
├── drive_downloads/            # Downloaded documents
└── logs/                       # System logs
```

## 🔧 Configuration

Edit `config/config.yaml` to customize:
- Download directories
- Retry settings
- Rate limits
- Monitoring thresholds

## 📊 CSV Schema

The system tracks downloads with these columns:
- `youtube_status`: Download status (pending/completed/failed)
- `youtube_files`: Downloaded filenames
- `drive_status`: Download status
- `drive_files`: Downloaded filenames
- `permanent_failure`: Marks content that should not be retried

## 🐛 Troubleshooting

### Common Issues

1. **YouTube download fails**: Ensure yt-dlp is up to date
   ```bash
   pip install --upgrade yt-dlp
   ```

2. **CSV corruption**: The system creates automatic backups
   ```bash
   ls outputs/backups/
   ```

3. **Permission errors**: Check file permissions in download directories

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Support

For issues and questions, please open an issue on GitHub.