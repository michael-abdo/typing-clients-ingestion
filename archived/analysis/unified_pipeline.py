#!/usr/bin/env python3
"""
Unified Pipeline Script - Executes the complete ingestion flow from Google Sheets to S3
This is the main entry point for the unified pipeline that coordinates all components.
"""

import sys
import os
import argparse
from datetime import datetime

# Add project path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from pipeline_orchestrator import PipelineOrchestrator
    from utils.logging_config import get_logger
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please ensure pipeline_orchestrator.py and utils/ directory are accessible.")
    sys.exit(1)

logger = get_logger(__name__)


def print_banner():
    """Print a nice banner for the unified pipeline"""
    print("\n" + "="*70)
    print("   UNIFIED INGESTION PIPELINE")
    print("   Google Sheets → Document Extraction → Downloads → S3 Upload")
    print("="*70)


def run_pipeline(pipeline_name: str = "full_ingestion", 
                 resume: bool = False,
                 dry_run: bool = False):
    """
    Run the specified pipeline
    
    Args:
        pipeline_name: Name of the pipeline to run (default: full_ingestion)
        resume: Whether to resume from last checkpoint
        dry_run: If True, only show what would be executed
    """
    print_banner()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(pipeline_name)
    
    if dry_run:
        print(f"\n🔍 DRY RUN MODE - Pipeline: {pipeline_name}")
        print("\nStages that would be executed:")
        for i, stage in enumerate(orchestrator.stages, 1):
            print(f"  {i}. {stage['name']}: {stage['description']}")
            print(f"     Command: {' '.join(stage['command'])}")
            print(f"     Timeout: {stage['timeout']}s")
        print("\nNo actual commands will be executed.")
        return
    
    # Show pipeline info
    print(f"\n📋 Pipeline: {pipeline_name}")
    print(f"🆔 Pipeline ID: {orchestrator.pipeline_id}")
    print(f"🔄 Mode: {'Resume from checkpoint' if resume else 'Fresh start'}")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # List stages
    print("\n📝 Pipeline Stages:")
    for i, stage in enumerate(orchestrator.stages, 1):
        print(f"  {i}. {stage['name']}: {stage['description']}")
    
    print("\n" + "-"*70)
    
    # Execute pipeline
    try:
        result = orchestrator.execute_pipeline(resume=resume)
        
        # Print results
        print("\n" + "="*70)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*70)
        print(f"Status: {result['status'].upper()}")
        
        if result.get('stages'):
            print("\n📈 Stage Results:")
            for stage_name, stage_info in result['stages'].items():
                status_icon = "✅" if stage_info['status'] == 'completed' else "❌"
                print(f"  {status_icon} {stage_name}: {stage_info['message']}")
        
        if result['status'] == 'completed':
            print("\n✅ SUCCESS! Pipeline completed successfully!")
            print("\n📦 What was accomplished:")
            print("  1. Extracted data from Google Sheets")
            print("  2. Processed Google Docs with HTTP extraction")
            print("  3. Downloaded YouTube videos and Drive files")
            print("  4. Uploaded all content to S3 bucket")
            
            # Check for S3 upload report
            if os.path.exists("s3_upload_report.json"):
                import json
                with open("s3_upload_report.json", 'r') as f:
                    s3_report = json.load(f)
                    if 'summary' in s3_report:
                        summary = s3_report['summary']
                        print(f"\n📊 S3 Upload Statistics:")
                        print(f"  - Total files: {summary.get('total_files', 'N/A')}")
                        print(f"  - Total size: {summary.get('total_size_mb', 'N/A')} MB")
                        print(f"  - Upload time: {summary.get('total_time_seconds', 'N/A')} seconds")
            
            # Cleanup if successful
            orchestrator.cleanup_state()
            
        else:
            print(f"\n❌ FAILURE! Pipeline failed: {result.get('error', 'Unknown error')}")
            print(f"\n💡 Tip: Run with --resume flag to retry from the failed stage")
            print(f"   Example: python3 unified_pipeline.py --resume")
        
        print("="*70 + "\n")
        
        return result['status'] == 'completed'
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        print("💡 Tip: Run with --resume flag to continue from where you left off")
        return False
    except Exception as e:
        logger.error(f"Pipeline execution error: {str(e)}")
        print(f"\n❌ Unexpected error: {str(e)}")
        return False


def check_status(pipeline_name: str = "full_ingestion"):
    """Check the status of a pipeline"""
    print_banner()
    
    # Try to find the most recent pipeline state file
    import glob
    state_files = glob.glob(f"pipeline_state_{pipeline_name}_*.json")
    
    if not state_files:
        print(f"\n❌ No pipeline state found for '{pipeline_name}'")
        return
    
    # Get the most recent state file
    latest_state = max(state_files, key=os.path.getmtime)
    
    import json
    with open(latest_state, 'r') as f:
        state = json.load(f)
    
    print(f"\n📋 Pipeline: {state.get('pipeline_name', 'N/A')}")
    print(f"🆔 Pipeline ID: {state.get('pipeline_id', 'N/A')}")
    print(f"📊 Status: {state.get('status', 'unknown').upper()}")
    print(f"🕐 Started: {state.get('started_at', 'N/A')}")
    
    if state.get('current_stage'):
        print(f"🔄 Current Stage: {state['current_stage']}")
    
    if state.get('stages'):
        print("\n📈 Stage Status:")
        for stage_name, stage_info in state['stages'].items():
            status_icon = "✅" if stage_info['status'] == 'completed' else "❌"
            print(f"  {status_icon} {stage_name}: {stage_info['status']}")
            if stage_info.get('message'):
                print(f"     {stage_info['message']}")


def main():
    """Main entry point for the unified pipeline"""
    parser = argparse.ArgumentParser(
        description='Unified Ingestion Pipeline - From Google Sheets to S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run the full pipeline
  python3 unified_pipeline.py
  
  # Resume from last checkpoint
  python3 unified_pipeline.py --resume
  
  # Check pipeline status
  python3 unified_pipeline.py --status
  
  # Dry run to see what would be executed
  python3 unified_pipeline.py --dry-run
  
  # Run a specific pipeline
  python3 unified_pipeline.py --pipeline test_extraction
        """
    )
    
    parser.add_argument('--pipeline', '-p', 
                       default='full_ingestion',
                       help='Pipeline to execute (default: full_ingestion)')
    parser.add_argument('--resume', '-r',
                       action='store_true',
                       help='Resume pipeline from last checkpoint')
    parser.add_argument('--status', '-s',
                       action='store_true',
                       help='Check pipeline status')
    parser.add_argument('--dry-run', '-d',
                       action='store_true',
                       help='Show what would be executed without running')
    parser.add_argument('--list-pipelines', '-l',
                       action='store_true',
                       help='List available pipelines')
    
    args = parser.parse_args()
    
    # Handle list pipelines
    if args.list_pipelines:
        from utils.config import get_config
        config = get_config()
        pipelines = config.get('pipelines', {})
        
        print_banner()
        print("\n📋 Available Pipelines:")
        for name, pipeline in pipelines.items():
            print(f"\n  • {name}: {pipeline.get('description', 'No description')}")
            if pipeline.get('stages'):
                print(f"    Stages: {len(pipeline['stages'])}")
        print()
        return
    
    # Handle status check
    if args.status:
        check_status(args.pipeline)
        return
    
    # Run the pipeline
    success = run_pipeline(
        pipeline_name=args.pipeline,
        resume=args.resume,
        dry_run=args.dry_run
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()