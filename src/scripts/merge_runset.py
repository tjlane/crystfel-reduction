#!/usr/bin/env python

import os
import argparse
import tempfile
from glob import glob
from pathlib import Path
from typing import Literal

from .. import utils
from .. import config


def launch_merge_job(
        *,
        name: str,
        runs: list[int],
        laser_state: Literal["light", "dark"],
        cfg: config.SwissFELConfig,
        queue: str = "week",
    ):

    if laser_state not in cfg.allowed_laser_states:
        raise ValueError(f"`laser_state` can only be {cfg.allowed_laser_states}")

    mrg = cfg.merging

    if mrg.use_online_streams:
        list_of_stream_paths: list[Path] = []
        for run in runs:
            pattern = f"/sf/{cfg.beamline}/data/{cfg.experiment_id}/res/run{run:04d}-*/index/{laser_state}/acq*.stream"
            list_of_stream_paths.extend(Path(p) for p in glob(pattern))
    else:
        list_of_stream_paths = [
            cfg.stream_file_directory / f"run{run:04d}" / f"run{run:04d}-{laser_state}.stream"
            for run in runs
        ]

    print(f"Wanted: {len(list_of_stream_paths)}")
    print(f"Found: {sum(p.exists() for p in list_of_stream_paths)} on disk")

    stream_paths_str = " ".join(str(p) for p in list_of_stream_paths)
    combine_stream_command = f"cat {stream_paths_str} > {name}_combined_{laser_state}.stream"

    sbatch_script_text = f"""#!/bin/sh

module purge
module load crystfel/{cfg.crystfel_version}

WD={cfg.merging_directory}/{name}
echo $WD
mkdir -p $WD
cd $WD

{combine_stream_command}

partialator -j $(nproc) -i {name}_combined_{laser_state}.stream -o {name}_{laser_state}.hkl \\
  -y {mrg.symmetry} --model={mrg.partiality_model} --iterations={mrg.partialator_iterations} \\
  --push-res={mrg.pushres} --max-adu={mrg.max_adu} > partialator.log 2>&1

check_hkl {name}_{laser_state}.hkl -y {mrg.symmetry} -p {cfg.cell_file_path} \\
  --highres={cfg.stats.stats_highres} --shell-file={name}_{laser_state}_check.dat

compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {mrg.symmetry} -p {cfg.cell_file_path} \\
  --highres={cfg.stats.stats_highres} --fom=rsplit --shell-file={name}_{laser_state}_rsplit.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {mrg.symmetry} -p {cfg.cell_file_path} \\
  --highres={cfg.stats.stats_highres} --fom=ccstar --shell-file={name}_{laser_state}_ccstar.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {mrg.symmetry} -p {cfg.cell_file_path} \\
  --highres={cfg.stats.stats_highres} --fom=cc --shell-file={name}_{laser_state}_cc.dat

mkdir stats
mv {name}_{laser_state}_check.dat {name}_{laser_state}_rsplit.dat {name}_{laser_state}_ccstar.dat {name}_{laser_state}_cc.dat stats/

get_hkl -i {name}_{laser_state}.hkl -y {mrg.symmetry} -p {cfg.cell_file_path} \\
  --output-format=mtz --highres={cfg.stats.stats_highres} -o {name}_{laser_state}.mtz

cp {name}_{laser_state}.mtz {cfg.mtz_directory}
"""

    with tempfile.TemporaryDirectory() as tempdir:
        cryst_run_file = os.path.join(tempdir, "merging_sbatch.sh")

        with open(cryst_run_file, "w") as run_sh:
            run_sh.write(sbatch_script_text)

        job_id = utils.submit_job(cryst_run_file, queue=queue, jobname="merging")

    return job_id


def main():

    parser = argparse.ArgumentParser(description="Merge a set of runs with partialator.")
    parser.add_argument("config", type=Path, help="Path to the YAML config file.")
    parser.add_argument("name", help="Dataset name used for output files.")
    parser.add_argument("runs", type=int, nargs="+", help="Run numbers to merge.")
    args = parser.parse_args()

    cfg = config.SwissFELConfig.from_yaml(args.config)

    for laser_state in ["dark", "light"]:
        launch_merge_job(
            name=args.name,
            runs=args.runs,
            laser_state=laser_state,
            cfg=cfg,
        )


if __name__ == "__main__":
    main()
