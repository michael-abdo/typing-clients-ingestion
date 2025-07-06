#!/usr/bin/env python3
"""
Performance Benchmark: CSV vs Database Write Speeds
Tests dual-write performance for database migration planning

Following CLAUDE.md principles: Smallest Feature, Fail FAST, Root Cause, DRY
"""

import time
import pandas as pd
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.csv_manager import CSVManager
from database.models.person import Person, PersonOperations
from utils.config import get_config

class PerformanceBenchmark:
    """Benchmark CSV vs Database write performance"""
    
    def __init__(self):
        self.config = get_config()
        self.test_output_dir = Path("benchmark_outputs")
        self.test_output_dir.mkdir(exist_ok=True)
        
        # Initialize database operations
        self.person_ops = PersonOperations()
        self.person_ops.create_table()
    
    def generate_test_data(self, num_records=100):
        """Generate test data for benchmarking"""
        print(f"📊 Generating {num_records} test records...")
        
        test_data = []
        for i in range(num_records):
            record = {
                'row_id': f"test_{i:04d}",
                'name': f"Test Person {i}",
                'email': f"test{i}@example.com",
                'type': f"Test-Type-{i % 5}",
                'link': f"https://example.com/doc_{i}" if i % 3 == 0 else ""
            }
            test_data.append(record)
        
        return test_data
    
    def benchmark_csv_write(self, test_data, iterations=1):
        """Benchmark CSV write performance"""
        print(f"📄 Benchmarking CSV write ({iterations} iterations)...")
        
        times = []
        for iteration in range(iterations):
            csv_file = self.test_output_dir / f"benchmark_csv_{iteration}.csv"
            df = pd.DataFrame(test_data)
            
            start_time = time.time()
            csv_manager = CSVManager(csv_path=str(csv_file))
            success = csv_manager.safe_csv_write(df, operation_name=f"benchmark_iteration_{iteration}")
            end_time = time.time()
            
            if success:
                times.append(end_time - start_time)
                print(f"  Iteration {iteration + 1}: {end_time - start_time:.4f}s")
            else:
                print(f"  Iteration {iteration + 1}: FAILED")
                return None
        
        avg_time = sum(times) / len(times)
        print(f"  ✅ CSV Average: {avg_time:.4f}s for {len(test_data)} records")
        return avg_time
    
    def benchmark_db_write(self, test_data, iterations=1):
        """Benchmark Database write performance"""
        print(f"🗄️  Benchmarking DB write ({iterations} iterations)...")
        
        times = []
        for iteration in range(iterations):
            # Convert test data to Person objects
            people = []
            for record in test_data:
                person = Person(
                    row_id=f"{record['row_id']}_iter_{iteration}",  # Unique row_id per iteration
                    name=record['name'],
                    email=record['email'] if record['email'] else None,
                    personality_type=record['type'] if record['type'] else None,
                    source_link=record['link'] if record['link'] else None
                )
                people.append(person)
            
            start_time = time.time()
            success_count = 0
            for person in people:
                if self.person_ops.insert_person(person):
                    success_count += 1
            end_time = time.time()
            
            if success_count == len(people):
                times.append(end_time - start_time)
                print(f"  Iteration {iteration + 1}: {end_time - start_time:.4f}s")
            else:
                print(f"  Iteration {iteration + 1}: PARTIAL FAILURE ({success_count}/{len(people)})")
                return None
        
        avg_time = sum(times) / len(times)
        print(f"  ✅ DB Average: {avg_time:.4f}s for {len(test_data)} records")
        return avg_time
    
    def run_benchmark(self, record_counts=[10, 50, 100], iterations=3):
        """Run comprehensive benchmark"""
        print("🏁 PERFORMANCE BENCHMARK: CSV vs Database")
        print("=" * 60)
        
        results = {}
        
        for count in record_counts:
            print(f"\n📏 Testing with {count} records ({iterations} iterations each):")
            print("-" * 50)
            
            # Generate test data
            test_data = self.generate_test_data(count)
            
            # Benchmark CSV
            csv_time = self.benchmark_csv_write(test_data, iterations)
            
            # Benchmark Database  
            db_time = self.benchmark_db_write(test_data, iterations)
            
            if csv_time and db_time:
                # Calculate performance metrics
                speedup = csv_time / db_time if db_time > 0 else 0
                csv_rps = count / csv_time if csv_time > 0 else 0
                db_rps = count / db_time if db_time > 0 else 0
                
                results[count] = {
                    'csv_time': csv_time,
                    'db_time': db_time,
                    'speedup': speedup,
                    'csv_rps': csv_rps,
                    'db_rps': db_rps
                }
                
                print(f"\n📈 Results for {count} records:")
                print(f"  CSV: {csv_time:.4f}s ({csv_rps:.1f} records/sec)")
                print(f"  DB:  {db_time:.4f}s ({db_rps:.1f} records/sec)")
                if speedup > 1:
                    print(f"  🚀 Database is {speedup:.2f}x FASTER than CSV")
                elif speedup < 1:
                    print(f"  🐌 CSV is {1/speedup:.2f}x FASTER than Database")
                else:
                    print(f"  ⚖️  Performance is roughly equal")
            else:
                print(f"❌ Benchmark failed for {count} records")
        
        return results
    
    def print_summary(self, results):
        """Print benchmark summary"""
        print("\n" + "=" * 60)
        print("📊 BENCHMARK SUMMARY")
        print("=" * 60)
        
        if not results:
            print("❌ No successful benchmark results")
            return
        
        print(f"{'Records':<10} {'CSV Time':<12} {'DB Time':<12} {'DB vs CSV':<15}")
        print("-" * 60)
        
        for count, data in results.items():
            csv_time = data['csv_time']
            db_time = data['db_time']
            speedup = data['speedup']
            
            if speedup > 1:
                comparison = f"{speedup:.2f}x faster"
            elif speedup < 1:
                comparison = f"{1/speedup:.2f}x slower"
            else:
                comparison = "~equal"
            
            print(f"{count:<10} {csv_time:.4f}s{'':<5} {db_time:.4f}s{'':<5} {comparison:<15}")
        
        # Overall recommendation
        avg_speedup = sum(data['speedup'] for data in results.values()) / len(results)
        print("\n💡 RECOMMENDATION:")
        if avg_speedup > 1.2:
            print("  ✅ Database writes are consistently faster - proceed with migration")
        elif avg_speedup < 0.8:
            print("  ⚠️  CSV writes are faster - consider optimizing database setup")
        else:
            print("  📊 Performance is similar - migration is viable for data integrity benefits")
        
        print(f"  📈 Average database speedup: {avg_speedup:.2f}x")

def main():
    """Run the performance benchmark"""
    benchmark = PerformanceBenchmark()
    
    # Run benchmark with different record counts
    results = benchmark.run_benchmark(
        record_counts=[10, 25, 50, 100],
        iterations=3
    )
    
    # Print summary
    benchmark.print_summary(results)
    
    return 0 if results else 1

if __name__ == "__main__":
    sys.exit(main())