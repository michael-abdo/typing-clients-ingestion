#!/usr/bin/env python3
"""
Comprehensive Regression Test for File Mapping Utilities
Tests all updated utilities to ensure consistent, contamination-free results.
"""

import os
import subprocess
import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set

class MappingRegressionTest:
    """Comprehensive test for all file mapping utilities"""
    
    def __init__(self):
        self.test_results = {}
        self.utilities = [
            'utils/clean_file_mapper.py',
            'utils/map_files_to_types.py', 
            'utils/comprehensive_file_mapper.py',
            'utils/csv_file_integrity_mapper.py',
            'utils/create_definitive_mapping.py'
        ]
        
        # Key contamination test cases
        self.contamination_tests = {
            '494': {'name': 'John Williams', 'expected_files': 4},
            '469': {'name': 'Ifrah Mohamed Mohamoud', 'expected_files': 0},
            '462': {'name': 'Miranda Story Ruiz', 'expected_files': 1}, 
            '492': {'name': 'Olivia Myers', 'expected_files': 2}
        }
    
    def run_all_tests(self) -> Dict:
        """Run comprehensive regression test on all utilities"""
        print("=== COMPREHENSIVE MAPPING UTILITIES REGRESSION TEST ===\n")
        
        # Test 1: Basic functionality
        print("1. Testing basic utility functionality...")
        self._test_utility_execution()
        
        # Test 2: File count consistency  
        print("\n2. Testing file count consistency...")
        self._test_file_count_consistency()
        
        # Test 3: Contamination elimination
        print("\n3. Testing contamination elimination...")
        self._test_contamination_elimination()
        
        # Test 4: CleanFileMapper integration
        print("\n4. Testing CleanFileMapper integration...")
        self._test_clean_mapper_integration()
        
        # Test 5: Row 494 specific fix validation
        print("\n5. Testing Row 494 contamination fix...")
        self._test_row_494_fix()
        
        return self._generate_final_report()
    
    def _test_utility_execution(self) -> None:
        """Test that all utilities execute without errors"""
        print("  Testing utility execution:")
        
        for utility in self.utilities:
            try:
                # Run utility and capture output
                result = subprocess.run(
                    ['python', utility], 
                    capture_output=True, 
                    text=True, 
                    timeout=60,
                    cwd=os.getcwd()
                )
                
                success = result.returncode == 0
                self.test_results[f"{utility}_execution"] = {
                    'success': success,
                    'stdout_lines': len(result.stdout.split('\n')),
                    'stderr': result.stderr if result.stderr else None
                }
                
                status = "✅ PASS" if success else "❌ FAIL"
                print(f"    {utility}: {status}")
                
                if not success:
                    print(f"      Error: {result.stderr}")
                    
            except Exception as e:
                self.test_results[f"{utility}_execution"] = {
                    'success': False,
                    'error': str(e)
                }
                print(f"    {utility}: ❌ FAIL - {e}")
    
    def _test_file_count_consistency(self) -> None:
        """Test that utilities report consistent file counts"""
        print("  Testing file count consistency:")
        
        # Run CleanFileMapper to get baseline
        from utils.clean_file_mapper import CleanFileMapper
        
        clean_mapper = CleanFileMapper()
        clean_mapper.map_all_files()
        baseline_count = len(clean_mapper.file_to_row)
        
        print(f"    Baseline (CleanFileMapper): {baseline_count} files")
        
        # Test other utilities report same count
        consistency_results = {}
        
        # Test map_files_to_types.py
        try:
            from utils.map_files_to_types import FileTypeMapper
            mapper = FileTypeMapper()
            mapper.scan_metadata_files()
            count = len(mapper.file_mapping)
            consistency_results['map_files_to_types'] = count
            print(f"    map_files_to_types.py: {count} files")
        except Exception as e:
            print(f"    map_files_to_types.py: ❌ ERROR - {e}")
        
        # Test create_definitive_mapping.py  
        try:
            from utils.create_definitive_mapping import DefinitiveMapper
            mapper = DefinitiveMapper()
            mapper.build_definitive_mapping()
            count = sum(len(m['all_files']) for m in mapper.definitive_mapping.values())
            consistency_results['create_definitive_mapping'] = count
            print(f"    create_definitive_mapping.py: {count} files")
        except Exception as e:
            print(f"    create_definitive_mapping.py: ❌ ERROR - {e}")
        
        # Check consistency
        all_consistent = all(count == baseline_count for count in consistency_results.values())
        status = "✅ CONSISTENT" if all_consistent else "❌ INCONSISTENT"
        print(f"    File count consistency: {status}")
        
        self.test_results['file_count_consistency'] = {
            'baseline': baseline_count,
            'results': consistency_results,
            'consistent': all_consistent
        }
    
    def _test_contamination_elimination(self) -> None:
        """Test that directory-based contamination is eliminated"""
        print("  Testing contamination elimination:")
        
        from utils.clean_file_mapper import CleanFileMapper
        clean_mapper = CleanFileMapper()
        clean_mapper.map_all_files()
        
        contamination_free = True
        
        for row_id, test_case in self.contamination_tests.items():
            files_for_row = clean_mapper.get_row_files(row_id)
            actual_count = len(files_for_row)
            expected_min = test_case['expected_files']
            
            # For contamination test, we care that files are in correct rows
            if row_id == '494':
                # Row 494 should have files (was contaminated to 469)
                test_passed = actual_count >= expected_min
            elif row_id == '469':
                # Row 469 should have no files (was contaminating others)
                test_passed = actual_count == 0
            else:
                # Other rows should have expected files
                test_passed = actual_count >= expected_min
            
            if not test_passed:
                contamination_free = False
            
            status = "✅ PASS" if test_passed else "❌ FAIL"
            print(f"    Row {row_id} ({test_case['name']}): {actual_count} files {status}")
        
        self.test_results['contamination_elimination'] = {
            'passed': contamination_free,
            'details': {row_id: len(clean_mapper.get_row_files(row_id)) 
                       for row_id in self.contamination_tests.keys()}
        }
        
        overall_status = "✅ ELIMINATED" if contamination_free else "❌ STILL PRESENT"
        print(f"    Overall contamination status: {overall_status}")
    
    def _test_clean_mapper_integration(self) -> None:
        """Test that all utilities properly integrate CleanFileMapper"""
        print("  Testing CleanFileMapper integration:")
        
        integration_tests = {}
        
        # Check that utilities import CleanFileMapper
        utilities_to_check = [
            'utils/map_files_to_types.py',
            'utils/comprehensive_file_mapper.py', 
            'utils/csv_file_integrity_mapper.py',
            'utils/create_definitive_mapping.py'
        ]
        
        for utility in utilities_to_check:
            try:
                with open(utility, 'r') as f:
                    content = f.read()
                    
                has_import = 'CleanFileMapper' in content
                has_clean_comment = 'contamination' in content.lower()
                has_updated_note = 'UPDATED:' in content
                
                integration_tests[utility] = {
                    'has_import': has_import,
                    'has_clean_comment': has_clean_comment, 
                    'has_updated_note': has_updated_note,
                    'integrated': has_import and has_clean_comment
                }
                
                status = "✅ INTEGRATED" if integration_tests[utility]['integrated'] else "❌ NOT INTEGRATED"
                print(f"    {utility}: {status}")
                
            except Exception as e:
                integration_tests[utility] = {'error': str(e)}
                print(f"    {utility}: ❌ ERROR - {e}")
        
        all_integrated = all(test.get('integrated', False) for test in integration_tests.values())
        self.test_results['clean_mapper_integration'] = {
            'all_integrated': all_integrated,
            'details': integration_tests
        }
    
    def _test_row_494_fix(self) -> None:
        """Specifically test that Row 494 contamination is fixed"""
        print("  Testing Row 494 contamination fix:")
        
        from utils.clean_file_mapper import CleanFileMapper
        clean_mapper = CleanFileMapper()
        clean_mapper.map_all_files()
        
        # Test Row 494 has files
        row_494_files = clean_mapper.get_row_files('494')
        has_files = len(row_494_files) > 0
        
        # Test Row 469 doesn't have Row 494's files
        row_469_files = clean_mapper.get_row_files('469')
        row_469_clean = len(row_469_files) == 0
        
        # Test specific files are correctly mapped
        test_files = [
            'youtube_downloads/ZBuff3DGbUM.mp4',
            'youtube_downloads/ZBuff3DGbUM_transcript.vtt', 
            'youtube_downloads/BtSNvQ9Rc90.mp4'
        ]
        
        correct_mappings = 0
        for test_file in test_files:
            mapped_row = clean_mapper.get_file_row(test_file)
            if mapped_row == '494':
                correct_mappings += 1
        
        row_494_fix_success = has_files and row_469_clean and (correct_mappings >= 2)
        
        print(f"    Row 494 has files: {len(row_494_files)} ✅" if has_files else f"    Row 494 has files: 0 ❌")
        print(f"    Row 469 is clean: {len(row_469_files)} files ✅" if row_469_clean else f"    Row 469 contaminated: {len(row_469_files)} files ❌")
        print(f"    Correct file mappings: {correct_mappings}/{len(test_files)} ✅" if correct_mappings >= 2 else f"    Correct file mappings: {correct_mappings}/{len(test_files)} ❌")
        
        self.test_results['row_494_fix'] = {
            'success': row_494_fix_success,
            'row_494_files': len(row_494_files),
            'row_469_files': len(row_469_files),
            'correct_mappings': correct_mappings,
            'total_test_files': len(test_files)
        }
        
        overall_status = "✅ FIXED" if row_494_fix_success else "❌ NOT FIXED"
        print(f"    Row 494 contamination fix: {overall_status}")
    
    def _generate_final_report(self) -> Dict:
        """Generate comprehensive final report"""
        print("\n=== FINAL REGRESSION TEST REPORT ===")
        
        # Count passed tests
        execution_tests = sum(1 for k, v in self.test_results.items() 
                            if k.endswith('_execution') and v.get('success', False))
        total_execution_tests = len([k for k in self.test_results.keys() if k.endswith('_execution')])
        
        consistency_passed = self.test_results.get('file_count_consistency', {}).get('consistent', False)
        contamination_passed = self.test_results.get('contamination_elimination', {}).get('passed', False)
        integration_passed = self.test_results.get('clean_mapper_integration', {}).get('all_integrated', False)
        row_494_passed = self.test_results.get('row_494_fix', {}).get('success', False)
        
        total_passed = sum([
            execution_tests == total_execution_tests,
            consistency_passed,
            contamination_passed, 
            integration_passed,
            row_494_passed
        ])
        
        print(f"Utility Execution: {execution_tests}/{total_execution_tests} ✅" if execution_tests == total_execution_tests else f"Utility Execution: {execution_tests}/{total_execution_tests} ❌")
        print(f"File Count Consistency: ✅" if consistency_passed else "File Count Consistency: ❌")  
        print(f"Contamination Elimination: ✅" if contamination_passed else "Contamination Elimination: ❌")
        print(f"CleanFileMapper Integration: ✅" if integration_passed else "CleanFileMapper Integration: ❌")
        print(f"Row 494 Fix Validation: ✅" if row_494_passed else "Row 494 Fix Validation: ❌")
        
        print(f"\nOVERALL RESULT: {total_passed}/5 tests passed")
        
        if total_passed == 5:
            print("🎉 ALL TESTS PASSED - Contamination elimination successful!")
        else:
            print("⚠️  Some tests failed - Review results above")
        
        final_report = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'overall_passed': total_passed == 5,
            'tests_passed': total_passed,
            'tests_total': 5,
            'detailed_results': self.test_results
        }
        
        # Save report
        with open('regression_test_report.json', 'w') as f:
            json.dump(final_report, f, indent=2, default=str)
        
        print(f"\nDetailed report saved to: regression_test_report.json")
        
        return final_report


if __name__ == "__main__":
    test = MappingRegressionTest()
    report = test.run_all_tests()