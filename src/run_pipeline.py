"""
Master Pipeline Runner
Run the complete data pipeline from end to end
"""

import subprocess
import sys
import os

# Force UTF-8 encoding for Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def check_dependencies():
    """Check if required libraries are installed"""
    print("Checking dependencies...")
    required = ['google_play_scraper', 'pandas', 'plotly']
    missing = []
    
    for package in required:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            missing.append(package)
            print(f"  ✗ {package} (missing)")
    
    if missing:
        print("\nMissing packages detected!")
        print("Install with: pip install " + " ".join(missing))
        response = input("\nWould you like to install them now? (y/n): ")
        if response.lower() == 'y':
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            print("✓ Dependencies installed!")
        else:
            print("Please install dependencies before running the pipeline.")
            sys.exit(1)
    else:
        print("✓ All dependencies installed\n")

def run_step(step_name, script_path):
    """Run a pipeline step using subprocess instead of exec"""
    print(f"RUNNING: {step_name}")
    
    try:
        # Use subprocess to run each script in a separate process
        # This preserves encoding settings
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=False,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=True
        )
        
        print(f"\n✓ {step_name} completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error in {step_name}:")
        print(f"   Script exited with error code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"\n✗ Error: Could not find {script_path}")
        print(f"   Make sure the file exists in the src/ directory")
        return False
    except Exception as e:
        print(f"\n✗ Unexpected error in {step_name}:")
        print(f"   {str(e)}")
        return False

def main():
    """Run the complete pipeline"""
    print("PYTHON DATA PIPELINE - COMPLETE RUN")
    print("\nThis script will run all pipeline steps in sequence:")
    print("  1. Data Ingestion (extract from Google Play)")
    print("  2. Data Transformation (clean and structure)")
    print("  3. Serving Layer (create analytics outputs)")
    print("  4. Dashboard (visualize results)")
    
    # Check dependencies first
    check_dependencies()
    
    # Prompt user
    response = input("\nReady to run the complete pipeline? (y/n): ")
    if response.lower() != 'y':
        print("Pipeline execution cancelled.")
        return
    
    # Define pipeline steps
    steps = [
        ("Step 1: Data Ingestion", "src/1_data_ingestion.py"),
        ("Step 2: Data Transformation", "src/2_data_transformation.py"),
        ("Step 3: Serving Layer", "src/3_serving_layer.py"),
        ("Step 4: Dashboard", "src/4_dashboard.py")
    ]
    
    # Run each step
    results = []
    for step_name, script_path in steps:
        success = run_step(step_name, script_path)
        results.append((step_name, success))
        
        if not success:
            print("PIPELINE STOPPED DUE TO ERROR")
            break
        
        # Pause between steps (except after the last one)
        if step_name != steps[-1][0]:
            input("Press Enter to continue to next step...")
    
    # Print final summary
    print("PIPELINE EXECUTION SUMMARY")
    for step_name, success in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{status}: {step_name}")
    
    if all(success for _, success in results):
        print("\n Complete pipeline executed successfully! :)")
        print("\nYour outputs are in:")
        print("  - data/raw/ (original data)")
        print("  - data/processed/ (cleaned data and dashboards)")
    else:
        print("\n Pipeline completed with errors.")
        print("Check the error messages above for details.")
    

if __name__ == "__main__":
    main()