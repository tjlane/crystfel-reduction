#!/usr/bin/env python

import os
import argparse
import tempfile
from pathlib import Path
from typing import Literal

from .. import utils
from .. import config


# TODO: move elsewhere?
def launch_merge_job(
        *,
        name: str,
        runs: list[int],
        laser_state: Literal["light", "dark"],
        config: config.SwissFELConfig,
        queue: str = "week",
    ):
    
    if laser_state not in config.allowed_laser_states:
        raise ValueError("`laser_state` can only be `light` or `dark`")
    
    list_of_stream_paths: list[Path] = []
    if config.use_online_streams:
        for run in runs:
            list_of_stream_paths.extend(Path(f"/sf/{config.beamline}/data/{config.experiment_id}/res/run{run:04d}-*/index/{laser_state}/acq*.stream").glob())
    else:
        list_of_stream_paths = [Path(f"{config.stream_file_directory}/run{run:04d}/run{run:04d}-{laser_state}.stream") for run in runs]

    print(f"Wanted: {len(list_of_stream_paths)}")
    print(f"Found: {sum([p.exists() for p in list_of_stream_paths])} on disk")

    stream_paths: str = " ".join()
    combine_stream_command = "cat " + stream_paths + f"> {name}_combined_{laser_state}.stream"

    sbatch_script_text = f"""#!/bin/sh

module purge
module load crystfel/{config.crystfel_version}

WD={config.merging_directory}/{name}
echo $WD
mkdir -p $WD
cd $WD

{combine_stream_command}

partialator -j {config.number_of_cores} -i {name}_combined_{laser_state}.stream -o {name}_{laser_state}.hkl -y {symmetry} --model={config.partiality_model} --iterations={config.partialator_iterations} --push-res={config.pushres} --max-adu={config.max_adu} > partialator.log 2>&1

check_hkl {name}_{laser_state}.hkl -y {config.symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --shell-file={name}_{laser_state}_check.dat

compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {config.symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=rsplit --shell-file={name}_{laser_state}_rsplit.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {config.symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=ccstar --shell-file={name}_{laser_state}_ccstar.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {config.symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=cc     --shell-file={name}_{laser_state}_cc.dat

mkdir stats
mv {name}_{laser_state}_check.dat {name}_{laser_state}_rsplit.dat {name}_{laser_state}_ccstar.dat {name}_{laser_state}_cc.dat stats/

get_hkl -i {name}_{laser_state}.hkl -y {config.symmetry} -p {config.cell_file_path} --output-format=mtz --highres={config.stats_highres} -o {name}_{laser_state}.mtz

cp {name}_{laser_state}.mtz {config.mtz_directory}
"""
    
    with tempfile.TemporaryDirectory() as tempdir:
        cryst_run_file = os.path.join(tempdir, f"merging_sbatch.sh")

        with open(cryst_run_file, "w") as run_sh:
            run_sh.write(sbatch_script_text)

        job_id = utils.submit_job(cryst_run_file, queue=queue, jobname="merging")

    # return crystfel file name
    return job_id


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("runs", type=int, nargs="+")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()

    for laser_state in ["dark", "light"]:
        launch_merge_job(
            name=args.name,
            runs=args.runs,
            laser_state=laser_state,
            config=args.config,
        )


if __name__ == "__main__":
    main()
