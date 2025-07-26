#!/usr/bin/env python3
"""
Minimal Pipeline Orchestrator - Chains existing components for full ingestion flow
Following DRY principles by reusing all existing functionality
"""

import subprocess
import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add project path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from utils.config import get_config
    from utils.logging_config import get_logger
    from utils.csv_manager import CSVManager
except ImportError:
    print("Error: Required utilities not found. Please ensure utils/ directory is accessible.")
    sys.exit(1)

logger = get_logger(__name__)
config = get_config()


class PipelineOrchestrator:
    """Minimal orchestrator to chain existing components"""
    
    def __init__(self, pipeline_name: str = "full_ingestion"):
        self.pipeline_name = pipeline_name
        self.pipeline_id = f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.state_file = Path(f"pipeline_state_{self.pipeline_id}.json")
        self.config = config
        self.logger = logger
        self.pipeline_settings = {}  # Initialize before loading stages
        
        # Pipeline stages configuration
        self.stages = self._get_pipeline_stages()
        
    def _get_pipeline_stages(self) -> List[Dict]:
        """Get pipeline stages from configuration"""
        # Get pipeline configuration from config.yaml
        pipelines = self.config.get('pipelines', {})
        pipeline_config = pipelines.get(self.pipeline_name, {})
        
        if not pipeline_config:
            self.logger.warning(f"Pipeline '{self.pipeline_name}' not found in config, using defaults")
            # Fall back to default configuration
            return [
                {
                    "name": "extract_data",
                    "description": "Extract data from Google Sheets and process documents",
                    "command": ["python3", "simple_workflow.py"],
                    "success_check": lambda: Path("outputs/output.csv").exists(),
                    "timeout": 600  # 10 minutes
                }
            ]
        
        # Parse stages from configuration
        stages = []
        for stage_config in pipeline_config.get('stages', []):
            if not stage_config.get('enabled', True):
                continue
                
            # Build success check function from markers
            success_checks = []
            for marker in stage_config.get('success_markers', []):
                if isinstance(marker, dict):
                    if 'file_exists' in marker:
                        path = marker['file_exists']
                        success_checks.append(lambda p=path: Path(p).exists())
                    elif 'directory_exists' in marker:
                        path = marker['directory_exists']
                        success_checks.append(lambda p=path: Path(p).exists() and Path(p).is_dir())
                    elif 'directory_not_empty' in marker:
                        path = marker['directory_not_empty']
                        success_checks.append(lambda p=path: Path(p).exists() and any(Path(p).iterdir()))
                elif isinstance(marker, str) and marker.startswith('file_exists:'):
                    path = marker.split(':', 1)[1]
                    success_checks.append(lambda p=path: Path(p).exists())
            
            # Combine all success checks
            def combined_check(checks=success_checks):
                return all(check() for check in checks) if checks else True
            
            stage = {
                "name": stage_config['name'],
                "description": stage_config.get('description', ''),
                "command": stage_config['command'],
                "timeout": stage_config.get('timeout', 600),
                "success_check": combined_check,
                "retry_on_failure": stage_config.get('retry_on_failure', False)
            }
            stages.append(stage)
        
        # Store pipeline settings for later use
        self.pipeline_settings = pipeline_config.get('settings', {})
        
        return stages
    
    def _load_state(self) -> Dict:
        """Load pipeline state from file"""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "started_at": datetime.now().isoformat(),
            "stages": {},
            "current_stage": None,
            "status": "pending"
        }
    
    def _save_state(self, state: Dict):
        """Save pipeline state to file"""
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def _execute_stage(self, stage: Dict) -> Tuple[bool, str]:
        """Execute a single pipeline stage"""
        stage_name = stage["name"]
        self.logger.info(f"Executing stage: {stage_name} - {stage['description']}")
        
        try:
            # Run the command
            start_time = time.time()
            result = subprocess.run(
                stage["command"],
                capture_output=True,
                text=True,
                timeout=stage.get("timeout", 600)
            )
            
            execution_time = time.time() - start_time
            
            # Check if command succeeded
            if result.returncode != 0:
                error_msg = f"Command failed with return code {result.returncode}\nError: {result.stderr}"
                self.logger.error(f"Stage {stage_name} failed: {error_msg}")
                return False, error_msg
            
            # Run success check if defined
            if "success_check" in stage:
                if not stage["success_check"]():
                    error_msg = "Success check failed - expected output not found"
                    self.logger.error(f"Stage {stage_name} success check failed")
                    return False, error_msg
            
            self.logger.info(f"Stage {stage_name} completed successfully in {execution_time:.2f} seconds")
            return True, f"Completed in {execution_time:.2f} seconds"
            
        except subprocess.TimeoutExpired:
            error_msg = f"Stage timed out after {stage.get('timeout', 600)} seconds"
            self.logger.error(f"Stage {stage_name} timeout: {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(f"Stage {stage_name} error: {error_msg}")
            return False, error_msg
    
    def execute_pipeline(self, resume: bool = False) -> Dict:
        """Execute the complete pipeline"""
        self.logger.info(f"Starting pipeline: {self.pipeline_name} (ID: {self.pipeline_id})")
        
        # Load state
        state = self._load_state()
        
        # Determine starting point
        start_index = 0
        if resume and state.get("current_stage"):
            # Find the stage to resume from
            for i, stage in enumerate(self.stages):
                if stage["name"] == state["current_stage"]:
                    start_index = i
                    break
            self.logger.info(f"Resuming from stage: {state['current_stage']}")
        
        # Execute stages
        state["status"] = "running"
        for i in range(start_index, len(self.stages)):
            stage = self.stages[i]
            stage_name = stage["name"]
            
            # Update current stage
            state["current_stage"] = stage_name
            self._save_state(state)
            
            # Execute stage
            success, message = self._execute_stage(stage)
            
            # Update state
            state["stages"][stage_name] = {
                "status": "completed" if success else "failed",
                "message": message,
                "executed_at": datetime.now().isoformat()
            }
            
            if not success:
                state["status"] = "failed"
                state["error"] = f"Failed at stage: {stage_name}"
                self._save_state(state)
                self.logger.error(f"Pipeline failed at stage: {stage_name}")
                return state
            
            self._save_state(state)
        
        # Pipeline completed successfully
        state["status"] = "completed"
        state["completed_at"] = datetime.now().isoformat()
        self._save_state(state)
        
        self.logger.info(f"Pipeline completed successfully!")
        return state
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status"""
        if self.state_file.exists():
            return self._load_state()
        return {"status": "not_started", "pipeline_id": self.pipeline_id}
    
    def cleanup_state(self):
        """Clean up state file after successful completion"""
        # Check if cleanup is enabled in pipeline settings
        cleanup_enabled = self.pipeline_settings.get('cleanup_on_success', True)
        
        if cleanup_enabled and self.state_file.exists():
            self.state_file.unlink()
            self.logger.info("Pipeline state cleaned up")
        elif not cleanup_enabled:
            self.logger.info("Pipeline state retained (cleanup_on_success=false)")


def main():
    """Main entry point for pipeline orchestration"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Pipeline Orchestrator')
    parser.add_argument('--pipeline', default='full_ingestion', 
                       help='Pipeline name to execute')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last checkpoint')
    parser.add_argument('--status', action='store_true',
                       help='Check pipeline status')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(args.pipeline)
    
    if args.status:
        # Show status
        status = orchestrator.get_pipeline_status()
        print(f"\nPipeline Status: {args.pipeline}")
        print(f"ID: {status.get('pipeline_id', 'N/A')}")
        print(f"Status: {status.get('status', 'not_started')}")
        print(f"Current Stage: {status.get('current_stage', 'N/A')}")
        
        if status.get('stages'):
            print("\nStage Status:")
            for stage_name, stage_info in status['stages'].items():
                print(f"  - {stage_name}: {stage_info['status']}")
    else:
        # Execute pipeline
        print(f"\n{'='*60}")
        print(f"UNIFIED PIPELINE ORCHESTRATOR")
        print(f"Pipeline: {args.pipeline}")
        print(f"Mode: {'Resume' if args.resume else 'Fresh Start'}")
        print(f"{'='*60}\n")
        
        result = orchestrator.execute_pipeline(resume=args.resume)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"PIPELINE EXECUTION SUMMARY")
        print(f"Status: {result['status']}")
        
        if result.get('stages'):
            print("\nStage Results:")
            for stage_name, stage_info in result['stages'].items():
                status_icon = "✅" if stage_info['status'] == 'completed' else "❌"
                print(f"  {status_icon} {stage_name}: {stage_info['message']}")
        
        if result['status'] == 'completed':
            print(f"\n✅ Pipeline completed successfully!")
            print(f"   - Google Sheets data extracted")
            print(f"   - Content downloaded locally")
            print(f"   - Files uploaded to S3")
            orchestrator.cleanup_state()
        else:
            print(f"\n❌ Pipeline failed: {result.get('error', 'Unknown error')}")
            print(f"   Run with --resume to retry from failed stage")
        
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()