#!/usr/bin/env python

import os
import argparse
import tempfile
from pathlib import Path

from .. import utils
from .. import swissfel

# TODO: online vs offline mode (!)

def launch_merge_job(
        *,
        name: str,
        runs: list[int],
        stream_location: Path,
        results_location: Path,
        mtz_location: Path,
        laser_state: str,
        config: swissfel.SwissFELConfig
    ):

    queue = "week"
    symmetry = "mmm"
    if laser_state not in ["light", "dark"]:
        raise ValueError("`laser_state` can only be `light` or `dark`")
    
    list_of_stream_paths = " ".join([f"{stream_location}/run{run:04d}/run{run:04d}-{laser_state}.stream " for run in runs])
    combine_stream_command = "cat " + list_of_stream_paths + f"> {name}_combined_{laser_state}.stream"

    sbatch_script_text = f"""#!/bin/sh

module purge
module load crystfel/{config.crystfel_version}

WD={results_location}/{name}
echo $WD
mkdir -p $WD
cd $WD

{combine_stream_command}

partialator -j {config.number_of_cores} -i {name}_combined_{laser_state}.stream -o {name}_{laser_state}.hkl -y {symmetry} --model={config.partiality_model} --iterations={config.partialator_iterations} --push-res={config.pushres} --max-adu={config.max_adu} > partialator.log 2>&1

check_hkl {name}_{laser_state}.hkl -y {symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --shell-file={name}_{laser_state}_check.dat

compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=rsplit --shell-file={name}_{laser_state}_rsplit.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=ccstar --shell-file={name}_{laser_state}_ccstar.dat
compare_hkl {name}_{laser_state}.hkl1 {name}_{laser_state}.hkl2 -y {symmetry} -p {config.cell_file_path} --highres={config.stats_highres} --fom=cc     --shell-file={name}_{laser_state}_cc.dat

mkdir stats
mv {name}_{laser_state}_check.dat {name}_{laser_state}_rsplit.dat {name}_{laser_state}_ccstar.dat {name}_{laser_state}_cc.dat stats/

get_hkl -i {name}_{laser_state}.hkl -y {symmetry} -p {config.cell_file_path} --output-format=mtz --highres={config.stats_highres} -o {name}_{laser_state}.mtz

cp {name}_{laser_state}.mtz {mtz_location}
"""
    
    with tempfile.TemporaryDirectory() as tempdir:
        cryst_run_file = os.path.join(tempdir, f"merging_sbatch.sh")

        with open(cryst_run_file, "w") as run_sh:
            run_sh.write(sbatch_script_text)

        job_id = utils.submit_job(cryst_run_file, queue=queue, jobname="merging")

    # return crystfel file name
    return job_id


def main():

    STATS_HIGRES=1.4

    STREAM_LOCATION="/sf/alvra/data/p21958/work/final_stream_files"
    # STREAM_LOCATION="/sf/alvra/data/p21958/work/stream_optimization/int_rad_2-3-6"
    RESULTS_LOCATION="/sf/alvra/data/p21958/work/final_merging"
    # RESULTS_LOCATION="/sf/alvra/data/p21958/work/merging-optimization"
    MTZ_LOCATION="/das/work/p21/p21958/final_mtzs"
    # MTZ_LOCATION="/das/work/p21/p21958/test_mtzs"

    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("runs", type=int, nargs="+")
    parser.add_argument("--laser", default="light", choices=["light", "dark"])
    args = parser.parse_args()

    launch_merge_job(
        name=args.name,
        runs=args.runs,
        cell_file=constants.CELL_FILE_PATH,
        stats_highres=STATS_HIGRES,
        stream_location=STREAM_LOCATION,
        results_location=RESULTS_LOCATION,
        mtz_location=MTZ_LOCATION,
        laser_state=args.laser,
    )


if __name__ == "__main__":
    main()
