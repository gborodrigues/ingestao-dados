import subprocess

def run_soda_scan():
    # Define the Soda scan command
    command = ["soda", "scan", "-d", "adventureworks", "-c", "configuration.yml", "checks.yml"]

    try:
        # Run the Soda scan command
        result = subprocess.run(command, capture_output=True, text=True)

        # Check if the command was successful
        if result.returncode == 0:
            print("Soda scan completed successfully.")
            print(result.stdout)  # Display the command output
        else:
            print("Soda scan failed.")
            print(result.stderr)  # Display the error output
    except Exception as e:
        print(f"An error occurred: {e}")

# Execute the function
run_soda_scan()