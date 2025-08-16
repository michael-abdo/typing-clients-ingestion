#!/usr/bin/env python3
"""
Final Data Consistency Validation (Phase 3)

Comprehensive data consistency validation between database and CSV backup
before transitioning to database-native operations. This is the final
verification step to ensure data integrity and completeness.

Validation Areas:
1. Record count comparison
2. Data integrity verification  
3. Field-by-field consistency checks
4. Missing data analysis
5. Data quality assessment
6. Final migration readiness report
"""

import os
import sys
import pandas as pd
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Set

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_native_operations import DatabaseNativeManager
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

class FinalDataConsistencyValidator:
    """Comprehensive final data consistency validation."""
    
    def __init__(self):
        """Initialize the final validator."""
        self.csv_path = "outputs/output.csv"
        self.db_manager = DatabaseNativeManager()
        self.csv_manager = CSVManager(self.csv_path)
        
        self.validation_results = {
            'timestamp': datetime.now(),
            'database_stats': {},
            'csv_stats': {},
            'consistency_checks': {},
            'data_quality': {},
            'migration_readiness': {},
            'recommendations': []
        }
        
        print("🔍 Final Data Consistency Validation (Phase 3)")
        print("=" * 50)
    
    def collect_database_statistics(self) -> Dict[str, Any]:
        """Collect comprehensive database statistics."""
        
        print("\n📊 Collecting Database Statistics...")
        
        try:
            # Basic counts
            total_records = self.db_manager.count_records()
            processed_records = len(self.db_manager.get_records_by_criteria(processed=True))
            unprocessed_records = len(self.db_manager.get_records_by_criteria(processed=False))
            
            # Get data sample for analysis
            sample_records = self.db_manager.get_records(limit=100)
            
            # Analyze data completeness
            field_completeness = {}
            if sample_records:
                for field in sample_records[0].keys():
                    non_null_count = sum(1 for record in sample_records if record.get(field) is not None)
                    field_completeness[field] = (non_null_count / len(sample_records)) * 100
            
            # Get row_id range
            all_records = self.db_manager.get_records()
            row_ids = [record['row_id'] for record in all_records if record.get('row_id')]
            
            db_stats = {
                'total_records': total_records,
                'processed_records': processed_records,
                'unprocessed_records': unprocessed_records,
                'min_row_id': min(row_ids) if row_ids else None,
                'max_row_id': max(row_ids) if row_ids else None,
                'unique_row_ids': len(set(row_ids)),
                'field_completeness': field_completeness,
                'sample_size': len(sample_records)
            }
            
            print(f"   ✅ Database: {total_records} records, {processed_records} processed")
            print(f"   📈 Row ID range: {db_stats['min_row_id']} to {db_stats['max_row_id']}")
            print(f"   🔍 Unique row IDs: {db_stats['unique_row_ids']}")
            
            self.validation_results['database_stats'] = db_stats
            return db_stats
            
        except Exception as e:
            print(f"   ❌ Database statistics collection failed: {e}")
            return {}
    
    def collect_csv_statistics(self) -> Dict[str, Any]:
        """Collect comprehensive CSV statistics."""
        
        print("\n📁 Collecting CSV Statistics...")
        
        try:
            # Read CSV data
            csv_df = self.csv_manager.read()
            
            # Basic counts
            total_records = len(csv_df)
            processed_records = len(csv_df[csv_df['processed'] == True]) if 'processed' in csv_df.columns else 0
            unprocessed_records = len(csv_df[csv_df['processed'] == False]) if 'processed' in csv_df.columns else 0
            
            # Analyze data completeness
            field_completeness = {}
            for field in csv_df.columns:
                non_null_count = csv_df[field].notna().sum()
                field_completeness[field] = (non_null_count / total_records) * 100 if total_records > 0 else 0
            
            # Get row_id range
            row_ids = csv_df['row_id'].dropna().tolist()
            if row_ids:
                # Convert to numeric if needed
                numeric_row_ids = []
                for rid in row_ids:
                    try:
                        numeric_row_ids.append(float(rid))
                    except:
                        pass
                row_ids = numeric_row_ids
            
            csv_stats = {
                'total_records': total_records,
                'processed_records': processed_records,
                'unprocessed_records': unprocessed_records,
                'min_row_id': min(row_ids) if row_ids else None,
                'max_row_id': max(row_ids) if row_ids else None,
                'unique_row_ids': len(set(row_ids)) if row_ids else 0,
                'field_completeness': field_completeness,
                'columns': list(csv_df.columns),
                'file_size_mb': self.csv_path.stat().st_size / (1024 * 1024) if Path(self.csv_path).exists() else 0
            }
            
            print(f"   ✅ CSV: {total_records} records, {processed_records} processed")
            print(f"   📈 Row ID range: {csv_stats['min_row_id']} to {csv_stats['max_row_id']}")
            print(f"   🔍 Unique row IDs: {csv_stats['unique_row_ids']}")
            print(f"   📄 File size: {csv_stats['file_size_mb']:.1f} MB")
            
            self.validation_results['csv_stats'] = csv_stats
            return csv_stats
            
        except Exception as e:
            print(f"   ❌ CSV statistics collection failed: {e}")
            return {}
    
    def validate_record_consistency(self) -> Dict[str, Any]:
        """Validate consistency of overlapping records."""
        
        print("\n🔄 Validating Record Consistency...")
        
        try:
            # Get overlapping data
            csv_df = self.csv_manager.read()
            csv_row_ids = set(csv_df['row_id'].dropna().astype(float).astype(int).tolist())
            
            db_records = self.db_manager.get_records()
            db_row_ids = set(record['row_id'] for record in db_records if record.get('row_id'))
            
            overlapping_ids = csv_row_ids.intersection(db_row_ids)
            csv_only_ids = csv_row_ids - db_row_ids  
            db_only_ids = db_row_ids - csv_row_ids
            
            print(f"   📊 Overlapping records: {len(overlapping_ids)}")
            print(f"   📁 CSV-only records: {len(csv_only_ids)}")
            print(f"   🗄️ Database-only records: {len(db_only_ids)}")
            
            # Validate overlapping records
            consistent_records = 0
            inconsistent_records = 0
            field_mismatches = {}
            
            # Test a sample of overlapping records
            test_ids = list(overlapping_ids)[:min(50, len(overlapping_ids))]
            
            for row_id in test_ids:
                # Get CSV record
                csv_record = csv_df[csv_df['row_id'] == float(row_id)]
                if csv_record.empty:
                    continue
                csv_data = csv_record.iloc[0].to_dict()
                
                # Get DB record
                db_data = self.db_manager.get_record_by_id(row_id)
                if not db_data:
                    continue
                
                # Compare key fields
                fields_to_check = ['name', 'email', 'type', 'processed']
                record_consistent = True
                
                for field in fields_to_check:
                    if field in csv_data and field in db_data:
                        csv_value = csv_data[field]
                        db_value = db_data[field]
                        
                        # Handle NaN/None comparisons
                        if pd.isna(csv_value) and db_value is None:
                            continue
                        if csv_value != db_value:
                            record_consistent = False
                            if field not in field_mismatches:
                                field_mismatches[field] = 0
                            field_mismatches[field] += 1
                
                if record_consistent:
                    consistent_records += 1
                else:
                    inconsistent_records += 1
            
            consistency_results = {
                'overlapping_count': len(overlapping_ids),
                'csv_only_count': len(csv_only_ids),
                'db_only_count': len(db_only_ids),
                'tested_records': len(test_ids),
                'consistent_records': consistent_records,
                'inconsistent_records': inconsistent_records,
                'consistency_rate': (consistent_records / len(test_ids) * 100) if test_ids else 100,
                'field_mismatches': field_mismatches
            }
            
            print(f"   ✅ Consistent records: {consistent_records}/{len(test_ids)} ({consistency_results['consistency_rate']:.1f}%)")
            if field_mismatches:
                print(f"   ⚠️ Field mismatches: {field_mismatches}")
            
            self.validation_results['consistency_checks'] = consistency_results
            return consistency_results
            
        except Exception as e:
            print(f"   ❌ Record consistency validation failed: {e}")
            return {}
    
    def assess_data_quality(self) -> Dict[str, Any]:
        """Assess overall data quality."""
        
        print("\n🔍 Assessing Data Quality...")
        
        try:
            # Database data quality
            db_records = self.db_manager.get_records()
            
            db_quality = {
                'total_records': len(db_records),
                'records_with_names': sum(1 for r in db_records if r.get('name')),
                'records_with_emails': sum(1 for r in db_records if r.get('email')),
                'duplicate_row_ids': len(db_records) - len(set(r['row_id'] for r in db_records if r.get('row_id'))),
                'null_row_ids': sum(1 for r in db_records if not r.get('row_id'))
            }
            
            # CSV data quality  
            csv_df = self.csv_manager.read()
            
            csv_quality = {
                'total_records': len(csv_df),
                'records_with_names': csv_df['name'].notna().sum() if 'name' in csv_df.columns else 0,
                'records_with_emails': csv_df['email'].notna().sum() if 'email' in csv_df.columns else 0,
                'duplicate_row_ids': csv_df['row_id'].duplicated().sum() if 'row_id' in csv_df.columns else 0,
                'null_row_ids': csv_df['row_id'].isna().sum() if 'row_id' in csv_df.columns else 0
            }
            
            # Overall quality assessment
            quality_scores = {
                'db_completeness': (db_quality['records_with_names'] / db_quality['total_records'] * 100) if db_quality['total_records'] > 0 else 0,
                'csv_completeness': (csv_quality['records_with_names'] / csv_quality['total_records'] * 100) if csv_quality['total_records'] > 0 else 0,
                'db_uniqueness': 100 - (db_quality['duplicate_row_ids'] / db_quality['total_records'] * 100) if db_quality['total_records'] > 0 else 100,
                'csv_uniqueness': 100 - (csv_quality['duplicate_row_ids'] / csv_quality['total_records'] * 100) if csv_quality['total_records'] > 0 else 100
            }
            
            quality_results = {
                'database_quality': db_quality,
                'csv_quality': csv_quality,
                'quality_scores': quality_scores
            }
            
            print(f"   📊 Database completeness: {quality_scores['db_completeness']:.1f}%")
            print(f"   📁 CSV completeness: {quality_scores['csv_completeness']:.1f}%")
            print(f"   🗄️ Database uniqueness: {quality_scores['db_uniqueness']:.1f}%")
            print(f"   📄 CSV uniqueness: {quality_scores['csv_uniqueness']:.1f}%")
            
            self.validation_results['data_quality'] = quality_results
            return quality_results
            
        except Exception as e:
            print(f"   ❌ Data quality assessment failed: {e}")
            return {}
    
    def assess_migration_readiness(self) -> Dict[str, Any]:
        """Assess readiness for database-native migration."""
        
        print("\n🎯 Assessing Migration Readiness...")
        
        try:
            # Collect all validation results
            db_stats = self.validation_results.get('database_stats', {})
            csv_stats = self.validation_results.get('csv_stats', {})
            consistency = self.validation_results.get('consistency_checks', {})
            quality = self.validation_results.get('data_quality', {})
            
            # Readiness criteria
            criteria = {
                'database_operational': db_stats.get('total_records', 0) > 0,
                'data_completeness': quality.get('quality_scores', {}).get('db_completeness', 0) >= 80,
                'data_uniqueness': quality.get('quality_scores', {}).get('db_uniqueness', 0) >= 99,
                'consistency_acceptable': consistency.get('consistency_rate', 0) >= 90,
                'sufficient_data': db_stats.get('total_records', 0) >= csv_stats.get('total_records', 0) * 0.8
            }
            
            # Calculate readiness score
            passed_criteria = sum(1 for passed in criteria.values() if passed)
            total_criteria = len(criteria)
            readiness_score = (passed_criteria / total_criteria * 100) if total_criteria > 0 else 0
            
            # Migration recommendation
            if readiness_score >= 100:
                recommendation = "READY - Proceed to database-native operations"
                status = "ready"
            elif readiness_score >= 80:
                recommendation = "MOSTLY READY - Minor issues to address"
                status = "mostly_ready"
            elif readiness_score >= 60:
                recommendation = "CAUTION - Significant issues need resolution"
                status = "caution"
            else:
                recommendation = "NOT READY - Critical issues must be resolved"
                status = "not_ready"
            
            readiness_results = {
                'criteria': criteria,
                'passed_criteria': passed_criteria,
                'total_criteria': total_criteria,
                'readiness_score': readiness_score,
                'status': status,
                'recommendation': recommendation
            }
            
            print(f"   📊 Readiness score: {readiness_score:.0f}% ({passed_criteria}/{total_criteria} criteria)")
            print(f"   🎯 Status: {status.upper()}")
            print(f"   💡 Recommendation: {recommendation}")
            
            # Print detailed criteria
            print(f"   📋 Criteria Details:")
            for criterion, passed in criteria.items():
                status_icon = "✅" if passed else "❌"
                print(f"      {status_icon} {criterion.replace('_', ' ').title()}")
            
            self.validation_results['migration_readiness'] = readiness_results
            return readiness_results
            
        except Exception as e:
            print(f"   ❌ Migration readiness assessment failed: {e}")
            return {}
    
    def generate_recommendations(self) -> List[str]:
        """Generate specific recommendations based on validation results."""
        
        recommendations = []
        
        # Check database statistics
        db_stats = self.validation_results.get('database_stats', {})
        csv_stats = self.validation_results.get('csv_stats', {})
        consistency = self.validation_results.get('consistency_checks', {})
        quality = self.validation_results.get('data_quality', {})
        readiness = self.validation_results.get('migration_readiness', {})
        
        # Database coverage recommendations
        if db_stats.get('total_records', 0) < csv_stats.get('total_records', 0):
            recommendations.append("Database contains fewer records than CSV - consider importing missing data")
        
        # Data quality recommendations  
        if quality.get('quality_scores', {}).get('db_completeness', 0) < 90:
            recommendations.append("Database has low completeness - review missing name/email fields")
        
        # Consistency recommendations
        if consistency.get('consistency_rate', 0) < 95:
            recommendations.append("Data consistency issues detected - investigate field mismatches")
        
        # Migration readiness recommendations
        if readiness.get('readiness_score', 0) < 90:
            recommendations.append("Address readiness criteria before proceeding to database-native operations")
        
        # Performance recommendations
        if db_stats.get('total_records', 0) > 1000:
            recommendations.append("Consider database performance optimization for large dataset")
        
        # Success recommendations
        if readiness.get('readiness_score', 0) >= 90:
            recommendations.append("✅ System ready for database-native operations")
            recommendations.append("✅ CSV backup can be safely archived")
            recommendations.append("✅ Proceed with final migration steps")
        
        if not recommendations:
            recommendations.append("No specific recommendations - validation completed successfully")
        
        self.validation_results['recommendations'] = recommendations
        return recommendations
    
    def generate_final_report(self) -> Dict[str, Any]:
        """Generate comprehensive final validation report."""
        
        print(f"\n📊 Final Data Consistency Validation Report")
        print("=" * 50)
        
        # Summary statistics
        db_stats = self.validation_results.get('database_stats', {})
        csv_stats = self.validation_results.get('csv_stats', {})
        readiness = self.validation_results.get('migration_readiness', {})
        
        print(f"\nData Summary:")
        print(f"  Database records: {db_stats.get('total_records', 'N/A')}")
        print(f"  CSV records: {csv_stats.get('total_records', 'N/A')}")
        print(f"  Data coverage: {(db_stats.get('total_records', 0) / max(csv_stats.get('total_records', 1), 1) * 100):.1f}%")
        
        # Readiness assessment
        print(f"\nMigration Readiness:")
        print(f"  Readiness score: {readiness.get('readiness_score', 0):.0f}%")
        print(f"  Status: {readiness.get('status', 'unknown').upper()}")
        print(f"  Recommendation: {readiness.get('recommendation', 'N/A')}")
        
        # Recommendations
        recommendations = self.generate_recommendations()
        print(f"\nRecommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        
        # Final assessment
        final_score = readiness.get('readiness_score', 0)
        print(f"\n💡 Final Assessment:")
        if final_score >= 90:
            print(f"✅ EXCELLENT - Database migration ready for completion")
            print(f"✅ Data integrity validated")
            print(f"✅ System ready for database-native operations")
        elif final_score >= 80:
            print(f"✅ GOOD - Minor issues identified but migration can proceed")
            print(f"⚠️ Monitor system closely during transition")
        else:
            print(f"⚠️ ISSUES DETECTED - Review recommendations before proceeding")
            print(f"🔧 Address data quality and consistency issues")
        
        return self.validation_results
    
    def run_full_validation(self) -> Dict[str, Any]:
        """Run complete final data consistency validation."""
        
        try:
            # Collect statistics
            self.collect_database_statistics()
            self.collect_csv_statistics()
            
            # Validate consistency
            self.validate_record_consistency()
            
            # Assess quality
            self.assess_data_quality()
            
            # Assess readiness
            self.assess_migration_readiness()
            
            # Generate final report
            final_results = self.generate_final_report()
            
            return final_results
            
        except Exception as e:
            print(f"\n💥 Final validation failed: {e}")
            db_logger.error(f"Final data consistency validation failed: {e}")
            return {}

def main():
    """Execute final data consistency validation."""
    
    validator = FinalDataConsistencyValidator()
    results = validator.run_full_validation()
    
    # Return success based on readiness score
    readiness_score = results.get('migration_readiness', {}).get('readiness_score', 0)
    return readiness_score >= 80

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)