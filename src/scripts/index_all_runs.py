#!/usr/bin/env python

import pandas as pd
from pathlib import Path

from cpdred.swissfel.proc import datalist
from cpdred.swissfel.proc import constants
from cpdred.swissfel.proc.index import launch_indexing_job


TARGET_DIR = Path(f"/sf/{constants.BEAMLINE}/data/{constants.EXPERIMENT_ID}/work/final_stream_files")

def index_run(run_number: int):

    if not TARGET_DIR.exists():
        TARGET_DIR.mkdir()

    for laser_state in ["light", "dark"]:

        list_file_path = datalist.get_combined_list_files_for_run(run_number, laser_state)

        geometry_file_path = constants.geometry_file_for_run(run_number)

        output_stream_dir = TARGET_DIR / Path(f"run{run_number:04d}")
        if not output_stream_dir.exists():
            output_stream_dir.mkdir()

        # recall: `list_file_path` is something like "acq????.JF06T08V04.{laser_state}.lst"
        output_stream_path = output_stream_dir / f"run{run_number:04d}-{laser_state}.stream"

        launch_indexing_job(
            list_file=list_file_path,
            geometry_file=geometry_file_path,
            cell_file=constants.CELL_FILE_PATH,
            output_stream_path=str(output_stream_path),
        )
    

if __name__ == "__main__":
    geometry_summary = pd.read_csv("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/geometry_summary.csv")
    runs = list(geometry_summary["run_number"])
    for run_number in runs:
        index_run(run_number)
