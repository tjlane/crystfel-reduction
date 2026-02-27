#!/usr/bin/env python

import argparse
import shutil
import numpy as np
from pathlib import Path

from .. import config, geometry


def optimize_run_geometry(run_number: int, cfg: config.SwissFELConfig):

    geo = cfg.geometry_optimization
    working_dir = cfg.geometry_optimization_directory / f"run{run_number:04d}"
    working_dir.mkdir(exist_ok=True)

    combined_list_path = working_dir / f"run{run_number:04d}_all_dark.lst"

    if not combined_list_path.exists():
        with combined_list_path.open("w") as outfile:
            for lst_file in config.get_list_files_for_run(run_number=run_number, config=cfg, laser_state="dark"):
                outfile.write(lst_file.read_text())

    clens_to_scan = np.arange(-geo.clen_half_range, geo.clen_half_range) * geo.step_size + geo.clen_center

    try:
        geometry.scan_for_optimal_geometry(
            working_dir=working_dir,
            list_file=combined_list_path,
            initial_geom_file=cfg.initial_geometry_file_path,
            cfg=cfg,
            clens_to_scan=clens_to_scan,
            subsample_size=geo.sample_size,
        )

        optimal_clen = geometry.determine_clen_from_scan(working_dir, plot=True)

        # apply x/y detector shift at the optimal clen
        clen_dir = working_dir / f"{optimal_clen:.5f}"
        clen_optimized_geometry_file = clen_dir / f"{optimal_clen:.5f}.geom"
        clen_optimized_stream = clen_dir / f"{optimal_clen:.5f}.stream"
        geometry.detector_shift(clen_optimized_geometry_file, [clen_optimized_stream])

        anticipated_optimal_geom_file = clen_dir / f"{optimal_clen:.5f}-predrefine.geom"
        nicely_named_final_geometry = working_dir / f"{run_number:04d}_optimized.geom"

        if anticipated_optimal_geom_file.exists():
            shutil.copy(anticipated_optimal_geom_file, nicely_named_final_geometry)

    except Exception as e:
        print(f" !!!  Error with run {run_number}... proceeding")
        print(e)
        print("")


def main():
    parser = argparse.ArgumentParser(description="Optimize detector geometry for each run.")
    parser.add_argument("config", type=Path, help="Path to the YAML config file.")
    args = parser.parse_args()

    cfg = config.SwissFELConfig.from_yaml(args.config)

    for run_number in range(*cfg.geometry_optimization.run_range):
        optimize_run_geometry(run_number, cfg)


if __name__ == "__main__":
    main()
