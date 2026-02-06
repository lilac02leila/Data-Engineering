"""
Run All Stress Test Scenarios
Master script to run all 5 scenarios and generate summary report
"""

import subprocess
import sys
import os
from datetime import datetime

def print_header(text):
    """Print formatted header"""
    print("\n" + "="*70)
    print(text.center(70))
    print("="*70)

def run_scenario(scenario_num, script_name, description):
    """Run a single scenario"""
    print_header(f"SCENARIO {scenario_num}: {description}")
    
    script_path = f"src/{script_name}"
    
    if not os.path.exists(script_path):
        print(f"✗ Error: {script_path} not found!")
        print(f"  Make sure the script is in the src/ directory")
        return False
    
    print(f"\nRunning: {script_path}")
    print("-" * 70)
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,
            text=True,
            check=True
        )
        print("\n" + "✓" * 35 + " SUCCESS " + "✓" * 35)
        return True
    except subprocess.CalledProcessError:
        print("\n" + "✗" * 35 + " FAILED " + "✗" * 35)
        return False
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

def main():
    """Main execution"""
    print_header("PART C: PIPELINE STRESS TESTING")
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("""
This will run all 5 stress test scenarios:
  1. New Reviews Batch
  2. Schema Drift
  3. Dirty Data
  4. Updated Apps Metadata
  5. New Business Logic (Sentiment Analysis)
    
    """)
    
    response = input("Run all scenarios? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Define scenarios
    scenarios = [
        (1, "scenario1_new_batch.py", "New Reviews Batch"),
        (2, "scenario2_schema_drift.py", "Schema Drift"),
        (3, "scenario3_dirty_data.py", "Dirty Data"),
        (4, "scenario4_updated_apps.py", "Updated Apps Metadata"),
        (5, "scenario5_sentiment_analysis.py", "Sentiment Analysis")
    ]
    
    # Track results
    results = []
    
    # Run each scenario
    for num, script, desc in scenarios:
        success = run_scenario(num, script, desc)
        results.append((num, desc, success))
        
        if not success:
            print(f"\n Scenario {num} failed. Continue anyway? (y/n): ")
            response = input()
            if response.lower() != 'y':
                print("Stopping execution.")
                break
        
        # Pause between scenarios
        if num < len(scenarios):
            print("\n" + "-" * 70)
            input("Press Enter to continue to next scenario...")
    
    # Print summary
    print_header("EXECUTION SUMMARY")
    
    print("\nResults:")
    for num, desc, success in results:
        status = "✓ COMPLETED" if success else "✗ FAILED"
        print(f"  Scenario {num} ({desc:30s}): {status}")
    
    success_count = sum(1 for _, _, s in results if s)
    total_count = len(results)
    
    print(f"\nTotal: {success_count}/{total_count} scenarios completed")
    
    if success_count == total_count:
        print("\n All scenarios completed successfully!")
    else:
        print(f"\n {total_count - success_count} scenario(s) failed")
    
    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    


if __name__ == "__main__":
    main()
