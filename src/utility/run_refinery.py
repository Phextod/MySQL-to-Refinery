import subprocess


def run_refinery(jar_path, input_path, output_path, random_seed: int = 1):
    print("Running Refinery")
    command = f"java -jar {jar_path} generate -o {output_path} -r {random_seed} {input_path}"
    try:
        subprocess.run(command, shell=True, check=True)
        print("Process finished successfully")
    except subprocess.CalledProcessError as e:
        print("Error executing command:", e)
