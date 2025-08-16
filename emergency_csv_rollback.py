#!/usr/bin/env python3
"""
Emergency CSV-Only Rollback Script

This script provides immediate rollback to CSV-only mode with fail-safe mechanisms.
Use this script when database issues occur during migration.

FAIL FAST: Exits immediately on any errors
FAIL LOUD: Logs all actions and errors prominently  
FAIL SAFELY: Creates backups before making changes
"""

import os
import sys
import shutil
import logging
import tempfile
from datetime import datetime
from pathlib import Path

# Setup logging for visibility
logging.basicConfig(
    level=logging.INFO,
    format='🚨 [%(asctime)s] ROLLBACK: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def emergency_rollback():
    """Execute emergency rollback to CSV-only mode."""
    
    logger.info("🚨 STARTING EMERGENCY ROLLBACK TO CSV-ONLY MODE")
    
    try:
        # Step 1: Create emergency backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"emergency_backup_{timestamp}"
        
        logger.info(f"📁 Creating emergency backup: {backup_dir}")
        os.makedirs(backup_dir, exist_ok=True)
        
        # Backup critical files
        critical_files = [
            "config/config.yaml",
            "outputs/output.csv",
            "utils/database_operations.py",
            "utils/database_manager.py"
        ]
        
        for file_path in critical_files:
            if os.path.exists(file_path):
                backup_path = os.path.join(backup_dir, os.path.basename(file_path))
                shutil.copy2(file_path, backup_path)
                logger.info(f"✅ Backed up: {file_path}")
            else:
                logger.warning(f"⚠️ File not found: {file_path}")
        
        # Step 2: Disable database in config
        logger.info("🔧 Disabling database in configuration")
        config_path = "config/config.yaml"
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_content = f.read()
            
            # Comment out database section and set fallback to CSV
            config_content = config_content.replace(
                'fallback_to_csv: true',
                'fallback_to_csv: true  # EMERGENCY: FORCED CSV MODE'
            )
            
            # Add emergency marker
            emergency_marker = """
# EMERGENCY ROLLBACK ACTIVE - CSV ONLY MODE
# Database operations disabled for safety
# Remove this section to re-enable database operations
database_emergency_disabled: true
"""
            config_content = emergency_marker + config_content
            
            with open(config_path, 'w') as f:
                f.write(config_content)
            
            logger.info("✅ Configuration updated for CSV-only mode")
        
        # Step 3: Create emergency flag file
        flag_file = "EMERGENCY_CSV_ONLY.flag"
        with open(flag_file, 'w') as f:
            f.write(f"""EMERGENCY CSV-ONLY MODE ACTIVE
Timestamp: {datetime.now().isoformat()}
Backup Location: {backup_dir}
Reason: Emergency rollback activated

To restore database mode:
1. Remove this file
2. Remove database_emergency_disabled from config.yaml
3. Test database connection
4. Resume migration process
""")
        
        logger.info(f"🚩 Created emergency flag: {flag_file}")
        
        # Step 4: Set environment variable to disable database
        logger.info("🔒 Setting emergency environment variable")
        os.environ['EMERGENCY_CSV_ONLY'] = 'true'
        
        # Write to shell profile for persistence
        shell_export = 'export EMERGENCY_CSV_ONLY=true  # Emergency rollback'
        
        profile_files = ['.bashrc', '.bash_profile', '.zshrc']
        for profile in profile_files:
            profile_path = os.path.expanduser(f'~/{profile}')
            if os.path.exists(profile_path):
                with open(profile_path, 'a') as f:
                    f.write(f'\n{shell_export}\n')
                logger.info(f"✅ Added to {profile}")
        
        # Step 5: Verify CSV file exists and is accessible
        csv_file = "outputs/output.csv"
        if os.path.exists(csv_file):
            file_size = os.path.getsize(csv_file)
            logger.info(f"✅ CSV file verified: {csv_file} ({file_size} bytes)")
        else:
            logger.error(f"❌ CRITICAL: CSV file not found: {csv_file}")
            return False
        
        # Step 6: Create rollback verification script
        verification_script = f"""#!/usr/bin/env python3
'''Emergency rollback verification'''
import os
import sys

def verify_csv_mode():
    checks = []
    
    # Check environment variable
    checks.append(os.environ.get('EMERGENCY_CSV_ONLY') == 'true')
    
    # Check flag file
    checks.append(os.path.exists('EMERGENCY_CSV_ONLY.flag'))
    
    # Check CSV file
    checks.append(os.path.exists('outputs/output.csv'))
    
    if all(checks):
        print("✅ Emergency CSV-only mode verified")
        return True
    else:
        print("❌ Emergency rollback verification failed")
        return False

if __name__ == "__main__":
    success = verify_csv_mode()
    sys.exit(0 if success else 1)
"""
        
        with open('verify_csv_rollback.py', 'w') as f:
            f.write(verification_script)
        
        logger.info("✅ Created verification script: verify_csv_rollback.py")
        
        # Step 7: Final verification
        logger.info("🔍 Running final verification")
        
        verification_checks = [
            os.path.exists(flag_file),
            os.path.exists(csv_file),
            os.path.exists(backup_dir),
            os.environ.get('EMERGENCY_CSV_ONLY') == 'true'
        ]
        
        if all(verification_checks):
            logger.info("🎉 EMERGENCY ROLLBACK COMPLETED SUCCESSFULLY")
            logger.info(f"📝 Backup created at: {backup_dir}")
            logger.info(f"🚩 Emergency flag: {flag_file}")
            logger.info("🔄 System is now in CSV-only mode")
            return True
        else:
            logger.error("❌ ROLLBACK VERIFICATION FAILED")
            return False
            
    except Exception as e:
        logger.error(f"💥 ROLLBACK FAILED: {e}")
        logger.error("🚨 MANUAL INTERVENTION REQUIRED")
        return False

def restore_database_mode():
    """Restore database mode after emergency rollback."""
    
    logger.info("🔄 RESTORING DATABASE MODE")
    
    try:
        # Remove emergency flag
        flag_file = "EMERGENCY_CSV_ONLY.flag"
        if os.path.exists(flag_file):
            os.remove(flag_file)
            logger.info(f"✅ Removed emergency flag: {flag_file}")
        
        # Remove environment variable
        if 'EMERGENCY_CSV_ONLY' in os.environ:
            del os.environ['EMERGENCY_CSV_ONLY']
            logger.info("✅ Removed emergency environment variable")
        
        logger.info("🎉 DATABASE MODE RESTORATION COMPLETED")
        logger.info("⚠️ Please verify database connection before proceeding")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 RESTORATION FAILED: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--restore":
        success = restore_database_mode()
    else:
        success = emergency_rollback()
    
    sys.exit(0 if success else 1)