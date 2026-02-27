#!/usr/bin/env python

import os
import shutil
import numpy as np

from cpdred.swissfel.proc import geometry, datalist, constants





def main():

    for run_number in range(*RUN_RANGE_OF_INTEREST):

        working_dir = os.path.join(BASE_LOCATION, f"run{run_number:04d}")

        if not os.path.exists(working_dir):
            print(f'making dir for run {run_number}')
            os.mkdir(working_dir)


        combined_list_path = os.path.join(working_dir, f"run{run_number:04d}_all_dark.lst")

        if not os.path.exists(combined_list_path):
            with open(combined_list_path, "w") as outfile:
                list_of_lst_files = datalist.get_list_files_for_run(run_number, laser_state="dark")
                for lst_file in list_of_lst_files:
                    with open(lst_file, "r") as readfile:
                        contents = readfile.read()   
                    outfile.write(contents)

        try:

            # first scan the detector distance
            # this blocks - scans the clen
            geometry.scan_for_optimal_geometry(
                working_dir=working_dir, 
                list_file=combined_list_path,
                subsample_size=SAMPLE_SIZE, 
                initial_geom_file=constants.INITIAL_GEOMETRY_FILE_PATH, 
                cell_file=constants.CELL_FILE_PATH, 
                clens_to_scan=CLENS_TO_SCAN,
            )

            optimal_clen = geometry.determine_clen_from_scan(working_dir, plot=True)

            # now, for the optimal, do x/y shift
            clen_optimized_geometry_file = os.path.join(working_dir, f"{optimal_clen:.5f}", f"{optimal_clen:.5f}.geom")
            clen_optimized_stream = os.path.join(working_dir, f"{optimal_clen:.5f}", f"{optimal_clen:.5f}.stream")
            geometry.detector_shift(clen_optimized_geometry_file, [clen_optimized_stream])

            anticipated_optimal_geom_file = os.path.join(working_dir, f"{optimal_clen:.5f}", f"{optimal_clen:.5f}-predrefine.geom")
            nicely_named_final_geometry = os.path.join(working_dir, f"{run_number:04d}_optimized.geom")

            if os.path.exists(anticipated_optimal_geom_file):
                shutil.copy(
                    anticipated_optimal_geom_file,
                    nicely_named_final_geometry
                )

        except Exception as exptn:
            print(f" !!!  Error with run {run_number}... proceeding")
            print(exptn)
            print("")


if __name__ == "__main__":
    main()

