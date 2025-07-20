import subprocess
import sys
import os
import time
import json

# --- Configuration ---
# Define the directories where your files are located
SRC_DIR = "src"
DATA_DIR = "data"
REPORTS_DIR = "reports"

# Define the sequence of scripts to run
SCRIPT_SEQUENCE = [
    "1_fetch_reddit_data.py",
    "2_prepare_ai_input.py",
    "3_get_ai_analysis.py",
    "4_generate_reports.py"
]

def run_script(script_name):
    """Executes a Python script located in the src directory."""
    script_path = os.path.join(SRC_DIR, script_name)
    print(f"--- Running {script_name}... ---")
    
    try:
        # sys.executable ensures we use the same Python interpreter
        result = subprocess.run([sys.executable, script_path], check=True, capture_output=True, text=True)
        print(result.stdout) # Print the script's output
        print(f"--- {script_name} COMPLETED successfully. ---\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"!!! ERROR: {script_name} failed to execute. !!!")
        print("--- Error Output ---")
        print(e.stderr)
        return False

def main():
    """Main controller to run the entire data processing pipeline."""
    print("=============================================")
    print("         REDDIT ANALYSIS PIPELINE         ")
    print("=============================================\n")

    # --- MODIFIED: Interactive menu to select starting step ---
    print("Please select the starting step for the pipeline:")
    print("  1: Fetch new Reddit data (Full Run)")
    print("  2: Prepare AI input (uses existing Reddit data)")
    print("  3: Get AI analysis (uses existing AI input file)")
    print("  4: Generate reports (uses existing AI analysis)")

    choice = 0
    while True:
        try:
            choice_str = input("Enter your choice (1-4): ")
            choice = int(choice_str)
            if 1 <= choice <= 4:
                break
            else:
                print("Invalid choice. Please enter a number between 1 and 4.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Slice the script sequence based on the user's choice
    scripts_to_run = SCRIPT_SEQUENCE[choice - 1:]

    # --- MODIFIED: Check if necessary input files exist for the chosen step ---
    required_files = {
        2: os.path.join(DATA_DIR, "south_asian_dating_reddit_data.json"),
        3: os.path.join(DATA_DIR, "ai_input_minimal.json"),
        4: os.path.join(DATA_DIR, "ai_analysis_output.json")
    }

    if choice in required_files and not os.path.exists(required_files[choice]):
        print(f"\n!!! ERROR: Cannot start at step {choice}. !!!")
        print(f"The required input file '{os.path.basename(required_files[choice])}' is missing from the '{DATA_DIR}' directory.")
        return

    print(f"\nStarting pipeline from: '{scripts_to_run[0]}'\n")
    # --- END OF MODIFICATIONS ---

    for script in scripts_to_run:
        # The crucial confirmation step before the OpenAI call
        if script == "3_get_ai_analysis.py":
            print("---------------------------------------------")
            print("  !! PENDING ACTION: OPENAI API CALL !!")
            print("---------------------------------------------")
            print("The next step will make a call to the OpenAI API, which will incur costs.")
            
            # Check if there's anything to process
            input_file_path = os.path.join(DATA_DIR, "ai_input_minimal.json")
            try:
                with open(input_file_path, 'r') as f:
                    data = json.load(f)
                if not data:
                    print("The input file 'ai_input_minimal.json' is empty. No new opportunities to analyze.")
                    print("Skipping OpenAI call and subsequent steps.")
                    break # Exit the loop
            except (FileNotFoundError, json.JSONDecodeError):
                 print(f"Warning: Could not read '{input_file_path}'. Assuming no data to process.")
                 break

            user_confirmation = input("Please type 'OK PASSED' to proceed: ")
            
            if user_confirmation.strip() == "OK PASSED":
                print("Confirmation received. Proceeding with API call...")
                time.sleep(1) 
            else:
                print("Confirmation not received. Halting the pipeline.")
                break 
        
        if not run_script(script):
            print("\nPipeline halted due to an error.")
            break 

    print("=============================================")
    print("          PIPELINE RUN FINISHED          ")
    print("=============================================")

if __name__ == "__main__":
    main()