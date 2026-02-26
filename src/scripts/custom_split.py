import re
import os
import glob
import subprocess
import sys



def find_event_integers(filename: str) -> list[int]:
    pattern = r'Event: //[-+]?\d+'
    matches = []

    with open(filename, 'r') as file:
        for _, line in enumerate(file, 1):
            for match in re.findall(pattern, line):
                number_str = match.split('//')[1]
                try:
                    number = int(number_str)
                    matches.append(number)
                except ValueError:
                    continue

    return matches



def glob_streams(tag: str, which: str) -> list[str]:
    pattern = f"/sf/alvra/data/p21958/res/run*-{tag}/index/{which}/acq*.stream"  
    return glob.glob(pattern)


def make_list(tag: str):

    with open("./custom-split.lst", "w") as f: # TODO: yaml
        for which in ["dark", "light"]: # TODO: yaml
            i = 0

            for stream in glob_streams(tag, which):
                stream = os.path.abspath(stream)
                pattern = r'Image filename:\s*(\S+)\s*Event:\s*(\S+)'

                with open(stream, "r") as stream_f:
                    matches = re.findall(pattern, stream_f.read())

                for image_filename, event in matches:
                    f.write(f"{image_filename} {event} {which}\n")
                    i += 1

            print(which, i)


def submit_partialator_job(tag):

    pattern = f"/sf/alvra/data/p21958/res/run*-{tag}/index/*/acq*.stream"

    text = f"""#!/bin/bash
#command for list with path:
#SBATCH --job-name crystfel
#SBATCH  -p week
#SBATCH --time 3-00:00:00
#SBATCH --exclusive
#

RUN={tag}
PUSHRES="2.2"

module clear
module use MX
module load crystfel/0.10.2


sbatch -p day --exclusive --reservation=p21958_2025-05-20 <<EOF
#!/bin/sh

source /etc/scripts/mx_fel.sh
NPROC=$(grep proc /proc/cpuinfo | wc -l )

partialator -j \$NPROC -i {pattern} --custom-split=custom-split.lst -o $RUN-$PUSHRES.hkl -y mmm --model=unity --iterations=1 --push-res=$PUSHRES > partialator_$RUN-$PUSHRES.log 2>&1

EOF
"""

    filename = "partialator_slurm_job.sh"
    with open(filename, 'w') as f:
        f.write(text)

    try:
        result = subprocess.run(["sbatch", filename], check=True, capture_output=True, text=True)
        print("Submission output:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Failed to submit job:")
        print(e.stderr)


def main(tag):
    make_list(tag)
    submit_partialator_job(tag)



# Example usage:
if __name__ == '__main__':
    tag = sys.argv[-1]
    print(tag)
    main(tag)

