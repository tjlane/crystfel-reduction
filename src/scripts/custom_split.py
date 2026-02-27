#!/usr/bin/env python

import re
import argparse
import subprocess
import tempfile
from glob import glob
from pathlib import Path

from .. import config


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


def glob_streams(tag: str, cfg: config.SwissFELConfig, which: str) -> list[str]:
    pattern = f"/sf/{cfg.beamline}/data/{cfg.experiment_id}/res/run*-{tag}/index/{which}/acq*.stream"
    return glob(pattern)


def make_list(tag: str, cfg: config.SwissFELConfig):

    with open("./custom-split.lst", "w") as f:
        for which in ["dark", "light"]:
            i = 0

            for stream in glob_streams(tag, cfg, which):
                stream = str(Path(stream).resolve())
                pattern = r'Image filename:\s*(\S+)\s*Event:\s*(\S+)'

                with open(stream, "r") as stream_f:
                    matches = re.findall(pattern, stream_f.read())

                for image_filename, event in matches:
                    f.write(f"{image_filename} {event} {which}\n")
                    i += 1

            print(which, i)


def submit_partialator_job(tag: str, cfg: config.SwissFELConfig):

    pattern = f"/sf/{cfg.beamline}/data/{cfg.experiment_id}/res/run*-{tag}/index/*/acq*.stream"
    mrg = cfg.merging

    script_text = f"""#!/bin/bash
#SBATCH --job-name crystfel
#SBATCH -p week
#SBATCH --time 3-00:00:00
#SBATCH --exclusive

module purge
module load crystfel/{cfg.crystfel_version}

partialator -j $(nproc) -i {pattern} --custom-split=custom-split.lst \\
  -o {tag}.hkl -y {mrg.symmetry} --model={mrg.partiality_model} \\
  --iterations={mrg.partialator_iterations} --push-res={mrg.pushres} \\
  --max-adu={mrg.max_adu} > partialator_{tag}.log 2>&1
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
        f.write(script_text)
        filename = f.name

    try:
        result = subprocess.run(["sbatch", filename], check=True, capture_output=True, text=True)
        print("Submission output:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Failed to submit job:")
        print(e.stderr)


def main():
    parser = argparse.ArgumentParser(description="Build a custom-split list and submit partialator.")
    parser.add_argument("config", type=Path, help="Path to the YAML config file.")
    parser.add_argument("tag", help="Run tag string.")
    args = parser.parse_args()

    cfg = config.SwissFELConfig.from_yaml(args.config)

    make_list(args.tag, cfg)
    submit_partialator_job(args.tag, cfg)


if __name__ == "__main__":
    main()
