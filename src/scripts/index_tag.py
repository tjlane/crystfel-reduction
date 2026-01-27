#!/usr/bin/env python

import re
import argparse
from pathlib import Path

from ..proc import geometry
from ..proc import datalist
from ..proc import constants
from ..proc.index import launch_indexing_job


TARGET_DIR = Path(f"/sf/{constants.BEAMLINE}/data/{constants.EXPERIMENT_ID}/work/final_stream_files")


def index_tag(tag: str):

    if tag not in datalist.CAN_TAGS_TO_ANALYZE + datalist.ECH_TAGS_TO_ANALYZE:
        raise RuntimeError(f"double check tag `{tag}` is valid")

    for laser_state in ["light", "dark"]:

        list_files = datalist.get_list_files_for_tag(tag, laser_state)
        
        for list_file_path in list_files:

            run_regex = re.search("run(\d\d\d\d)", list_file_path)
            run_number = int(run_regex.group(1))

            geometry_file_path = geometry.geometry_file_for_run(run_number)

            output_stream_dir = TARGET_DIR / Path(f"{tag}")
            if not output_stream_dir.exists():
                output_stream_dir.mkdir()

            # recall: `list_file_path` is something like "acq????.JF06T08V04.{laser_state}.lst"
            list_file_name = Path(list_file_path).name[:-4]  # just "acq????.JF06T08V04.{laser_state}"
            output_stream_path = output_stream_dir / f"{tag}_run{run_number:04d}_{list_file_name}.stream"

            launch_indexing_job(
                list_file=list_file_path,
                geometry_file=geometry_file_path,
                cell_file=CELL_FILE,
                output_stream_path=str(output_stream_path),
            )
    

if __name__ == "__main__":
    raise NotImplementedError("never updated from OCP")
    parser = argparse.ArgumentParser()
    parser.add_argument('tag', help="tag to index, such as `ocp_100ns_7um_5mJmm2`")
    args = parser.parse_args()
    index_tag(args.tag)
