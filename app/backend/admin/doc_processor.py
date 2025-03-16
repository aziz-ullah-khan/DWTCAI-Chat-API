import os
import shutil
import asyncio
import ast

# Function to run a shell command and capture its output
import asyncio
import re

def extract_filenames(stdout_lines):
    pattern = r"Ingesting '(.+?)'"  # Regex to match file names
    filenames = []  # List to store all filenames
    
    for line in stdout_lines:
        match = re.search(pattern, line)
        if match:
            filenames.append(match.group(1))  # Add each filename to the list

    return filenames  # Return the complete list

async def run_command(command):
    try:
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout_lines = []
        while True:
            stdout_line = await process.stdout.readline()
            stderr_line = await process.stderr.readline()
            if not stdout_line and not stderr_line:
                break  # Exit the loop when there's no more output
            if stdout_line:
                print(f"STDOUT: {stdout_line.decode().strip()}")
                stdout_lines.append(stdout_line.decode().strip())
            if stderr_line:
                print(f"STDERR: {stderr_line.decode().strip()}")
        
        returncode = await process.wait()

        if returncode != 0:
            raise Exception(f"Error running command: {command}\nReturn code: {returncode}")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)


    return stdout_lines

# Loading .env file from current environment if not already loaded
if "AZURE_STORAGE_ACCOUNT" not in os.environ:
    print("Loading azd .env file from current environment")
    env_values = os.popen("azd env get-values").read()
    # Parsing and exporting environment variables
    for line in env_values.splitlines():
        key, value = line.split('=', 1)  
        value = value.strip('"')
        os.environ[key] = value

async def prepdocs_processor(files_dir, container, index, max_depth, url=None):
    # Running "prepdocs.py" with user-provided values
    print('Running "prepdocs.py"')

    if url:
        command = f'python ./prepdocs.py "{url}" --url --max_depth {max_depth} --storagekey "$AZURE_STORAGE_KEY" --container "{container}" --searchservice "$AZURE_SEARCH_SERVICE" --searchkey "$AZURE_SEARCH_KEY" --index "{index}" -v'
        stdout_lines = await run_command(command)
    else:
        command = f'python ./prepdocs.py "{files_dir}/*"  --storagekey "$AZURE_STORAGE_KEY" --container "{container}" --searchservice "$AZURE_SEARCH_SERVICE" --searchkey "$AZURE_SEARCH_KEY" --index "{index}" -v'
        stdout_lines = await run_command(command)


    filename = extract_filenames(stdout_lines)
    return filename

    # print("STDOUT OUTPUT:", stdout_lines)  # Print full output for debugging

    # if len(stdout_lines) < 2:
    #     raise ValueError("Unexpected output format from prepdocs.py. Output too short.")

    # try:
    #     return ast.literal_eval(stdout_lines[-2])  # Ensure it's a valid Python literal
    # except Exception as e:
    #     raise ValueError(f"Error parsing prepdocs output: {e}. Raw output: {stdout_lines[-2]}")

    # return ast.literal_eval(stdout_lines[-2])

