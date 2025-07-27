# Deployment Checklist & Guide

## Pre-Deployment Checklist

### 🔧 System Requirements
- [ ] Python 3.8+ installed
- [ ] Chrome/Chromium browser available
- [ ] ChromeDriver installed and in PATH
- [ ] Network access to Google services
- [ ] AWS credentials configured (if using S3)
- [ ] Database accessible (if using database features)

### 📋 Configuration Checklist
- [ ] `.env` file created and configured
- [ ] `config/config.yaml` reviewed and customized
- [ ] Database connection tested (if applicable)
- [ ] S3 bucket permissions verified (if applicable)
- [ ] Google Sheets URL configured correctly
- [ ] Rate limiting settings appropriate for your use case

### 🔍 Pre-Deployment Validation
```bash
# 1. Run health check
python3 health_check.py

# 2. Test basic functionality
python3 simple_workflow.py --basic --test-limit 1

# 3. Verify dependencies
pip install -r requirements.txt

# 4. Check configuration
python3 -c "from utils.config import get_config; print('Config OK')"
```

## Deployment Steps

### Option 1: Quick Setup (Recommended)
```bash
git clone <repository-url>
cd typing-clients-ingestion-minimal
./setup.sh
# Follow prompts and edit .env file
python3 health_check.py
```

### Option 2: Manual Setup
```bash
# 1. Clone repository
git clone <repository-url>
cd typing-clients-ingestion-minimal

# 2. Install dependencies
pip3 install -r requirements.txt

# 3. Configure environment
cp .env.example .env
cp config/config.yaml.example config/config.yaml

# 4. Edit configuration files
nano .env                    # Add your credentials
nano config/config.yaml     # Customize settings

# 5. Create directories
mkdir -p outputs cache logs

# 6. Validate setup
python3 health_check.py
```

## Environment-Specific Configurations

### Development Environment
```bash
# .env settings for development
DEBUG=true
LOG_LEVEL=DEBUG
HEADLESS_MODE=false
TEST_MODE=true
```

### Production Environment
```bash
# .env settings for production
DEBUG=false
LOG_LEVEL=INFO
HEADLESS_MODE=true
MAX_REQUESTS_PER_MINUTE=30
```

### CI/CD Environment
```bash
# .env settings for automated testing
DEBUG=false
LOG_LEVEL=WARNING
HEADLESS_MODE=true
TEST_MODE=true
```

## Post-Deployment Verification

### 1. Smoke Tests
```bash
# Test core functionality
python3 simple_workflow.py --help
python3 health_check.py

# Test basic workflow
python3 simple_workflow.py --basic --test-limit 1

# Check output
ls -la outputs/
```

### 2. Integration Tests
```bash
# Test with real data (small sample)
python3 simple_workflow.py --basic --test-limit 5

# Verify CSV output
head -5 outputs/output.csv

# Check S3 upload (if configured)
# Review logs for S3 success/failure messages
```

### 3. Performance Validation
```bash
# Monitor resource usage during execution
python3 simple_workflow.py --basic --test-limit 10

# Check memory and CPU usage
# Verify reasonable execution times
```

## Monitoring & Maintenance

### Log Monitoring
- **Location**: `logs/` directory
- **Key files**: `logs/runs/latest/`
- **Watch for**: Error patterns, performance issues, rate limiting

### CSV Output Monitoring
- **Location**: `outputs/output.csv`
- **S3 Backups**: Automatic versioning (if configured)
- **Validation**: Check row counts and data quality

### Health Monitoring
```bash
# Regular health checks
python3 health_check.py

# Monitor workflow success rates
grep "WORKFLOW COMPLETE" logs/runs/latest/*.log
```

## Troubleshooting Guide

### Common Issues

**Import Errors:**
```bash
# Solution: Reinstall dependencies
pip3 install -r requirements.txt --force-reinstall
```

**ChromeDriver Issues:**
```bash
# Solution: Update ChromeDriver
pip3 install webdriver-manager --upgrade
```

**Configuration Errors:**
```bash
# Solution: Validate config files
python3 -c "from utils.config import get_config; get_config()"
```

**S3 Access Denied:**
```bash
# Solution: Check AWS credentials
aws s3 ls s3://your-bucket-name/
```

**Database Connection Failed:**
```bash
# Solution: Test database connectivity
python3 -c "from utils.database_operations import test_connection; test_connection()"
```

### Performance Issues

**Slow Execution:**
- Reduce batch size in config
- Increase delays between requests
- Check network connectivity
- Monitor system resources

**Memory Usage:**
- Use `--test-limit` for large datasets
- Process in smaller batches
- Monitor CSV file sizes

## Security Considerations

### Production Security
- [ ] All credentials in environment variables
- [ ] No hardcoded secrets in code
- [ ] Database passwords secured
- [ ] S3 bucket permissions minimal
- [ ] Rate limiting configured appropriately

### Network Security
- [ ] HTTPS used for all external connections
- [ ] Domain filtering enabled for URL validation
- [ ] Input sanitization active
- [ ] Error messages don't leak sensitive info

## Backup & Recovery

### Data Backup
- **CSV files**: Automatic S3 versioning
- **Configuration**: Version control (.env excluded)
- **State files**: Regular backup of progress JSON

### Recovery Procedures
```bash
# Restore from S3 backup
aws s3 cp s3://bucket/csv-versions/latest.csv outputs/output.csv

# Resume interrupted processing
python3 simple_workflow.py --resume

# Retry failed extractions
python3 simple_workflow.py --retry-failed
```

## Scaling Considerations

### High-Volume Processing
- Use batch processing (`--batch-size`)
- Implement queue-based processing
- Consider distributed execution
- Monitor rate limits carefully

### Multi-Environment Deployment
- Separate S3 buckets per environment
- Environment-specific configuration
- Isolated database schemas
- Separate monitoring dashboards

---

*Run `python3 health_check.py` before any deployment to ensure system readiness.*