# Typing Clients Ingestion Pipeline

A comprehensive data extraction and processing pipeline for Google Sheets, Google Docs, YouTube, and Google Drive content with advanced DRY (Don't Repeat Yourself) architecture.

## Features

- **Multi-source Data Extraction**: Google Sheets, Google Docs, YouTube videos, Google Drive files
- **Flexible Processing Modes**: Basic, full, and text extraction workflows
- **S3 Integration**: Direct streaming uploads with versioning
- **Robust Error Handling**: Retry mechanisms with exponential backoff
- **Security-First**: Input validation, CSV sanitization, URL filtering
- **DRY Architecture**: Consolidated patterns eliminate code duplication

## Quick Start

### Prerequisites

- Python 3.8+
- Chrome/Chromium browser (for Selenium)
- ChromeDriver (for web scraping)
- AWS credentials (optional, for S3 features)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd typing-clients-ingestion-minimal
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Set up configuration**
   ```bash
   cp config/config.yaml.example config/config.yaml
   # Edit config.yaml as needed
   ```

### Basic Usage

**Run health check first:**
```bash
python3 health_check.py
```

**Extract basic data only:**
```bash
python3 simple_workflow.py --basic --test-limit 10
```

**Full processing with document extraction:**
```bash
python3 simple_workflow.py --test-limit 5
```

**Text extraction mode:**
```bash
python3 simple_workflow.py --text --batch-size 5
```

## Configuration

### Environment Variables

Required environment variables (see `.env.example`):

- `DB_PASSWORD`: Database password
- `AWS_ACCESS_KEY_ID`: AWS access key (optional)
- `AWS_SECRET_ACCESS_KEY`: AWS secret key (optional)
- `S3_BUCKET`: S3 bucket name (optional)

### Main Configuration

Edit `config/config.yaml` to customize:

- Google Sheets URL and target div
- File paths and directories
- Download settings and storage mode
- S3 configuration
- Rate limiting and timeouts

## Project Structure

```
├── simple_workflow.py      # Main workflow orchestrator
├── config/                 # Configuration files
├── utils/                  # Core utilities (DRY consolidated)
│   ├── config.py          # Centralized configuration
│   ├── csv_manager.py     # CSV operations
│   ├── validation.py      # Security validation
│   ├── s3_manager.py      # S3 operations
│   └── ...
├── core/                  # Core processing modules
├── tests/                 # Test suites
└── outputs/               # Generated output files
```

## Workflow Modes

### Basic Mode (`--basic`)
- Extracts only: row_id, name, email, type, link
- Fast processing, minimal resource usage
- Best for data validation and quick analysis

### Full Mode (default)
- Complete document processing
- Downloads and processes all linked content
- Includes metadata extraction and S3 upload

### Text Mode (`--text`)
- Batch processing of documents
- Extracts full text content from Google Docs
- Optimized for content analysis workflows

## Advanced Features

### DRY Architecture Benefits
- **Single Source of Truth**: All patterns consolidated
- **Consistent Error Handling**: Unified exception management
- **Security by Design**: Centralized validation and sanitization
- **Performance Optimized**: Eliminated duplicate operations

### Security Features
- CSV formula injection protection
- URL validation with domain filtering
- Input sanitization for all user data
- Environment-based credential management

### Monitoring & Logging
- Comprehensive logging with timestamps
- Progress tracking with JSON state files
- CSV versioning with S3 backup
- Error categorization and retry logic

## Development

### Running Tests
```bash
python3 tests/run_all_tests.py
```

### Code Structure
The codebase follows DRY principles with consolidated patterns:
- Configuration management in `utils/config.py`
- All CSV operations in `utils/csv_manager.py`
- Security validation in `utils/validation.py`
- Retry logic in `utils/retry_utils.py`

## Troubleshooting

### Common Issues

**Import Errors:**
- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check Python version: requires Python 3.8+

**ChromeDriver Issues:**
- Install ChromeDriver: `brew install chromedriver` (macOS) or download manually
- Set `SELENIUM_DRIVER_PATH` in `.env` if needed

**S3 Access Denied:**
- Configure AWS credentials in `.env`
- Verify S3 bucket permissions
- Check bucket name in `config/config.yaml`

**Database Connection:**
- Set `DB_PASSWORD` in `.env`
- Ensure database is running and accessible
- Check connection settings in `config/config.yaml`

### Performance Optimization

- Use `--test-limit` for development/testing
- Adjust `--batch-size` for text extraction
- Configure rate limiting in `config.yaml`
- Monitor S3 costs with versioning

## Contributing

1. Follow existing DRY patterns
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure security validation for all inputs

## License

[Add appropriate license]

## Support

For issues and questions, please check:
1. This README for common solutions
2. Configuration files for proper setup
3. Log files for detailed error information

---

*Generated with DRY-optimized architecture for maximum maintainability and performance.*