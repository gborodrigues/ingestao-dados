import subprocess

def run_soda_scan():

    command = ["soda", "scan", "-d", "adventureworks", "-c", "configuration.yml", "checks.yml"]

    try:
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            print("Soda scan completed successfully.")
            print(result.stdout)  
        else:
            print("Soda scan failed.")
            print(result.stderr)  
    except Exception as e:
        print(f"An error occurred: {e}")

run_soda_scan()