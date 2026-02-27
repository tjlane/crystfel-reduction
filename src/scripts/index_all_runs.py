#!/usr/bin/env python

import argparse
import pandas as pd
from pathlib import Path

from .. import config, geometry, index


def index_run(run_number: int, cfg: config.SwissFELConfig):

    target_dir = cfg.stream_file_directory
    target_dir.mkdir(exist_ok=True)

    for laser_state in cfg.allowed_laser_states:

        list_file_path = config.get_combined_list_files_for_run(run_number=run_number, config=cfg, laser_state=laser_state)

        geometry_file_path = geometry.geometry_file_for_run(run_number, cfg)

        output_stream_dir = target_dir / f"run{run_number:04d}"
        output_stream_dir.mkdir(exist_ok=True)

        output_stream_path = output_stream_dir / f"run{run_number:04d}-{laser_state}.stream"

        index.launch_indexing_job(
            list_file=list_file_path,
            geometry_file=geometry_file_path,
            output_stream_path=output_stream_path,
            config=cfg,
        )


def main():
    parser = argparse.ArgumentParser(description="Index all runs using a SwissFEL config.")
    parser.add_argument("config", type=Path, help="Path to the YAML config file.")
    args = parser.parse_args()

    cfg = config.SwissFELConfig.from_yaml(args.config)

    geometry_summary = pd.read_csv(cfg.geometry_summary_path)
    runs = list(geometry_summary["run_number"])
    for run_number in runs:
        index_run(run_number, cfg)


if __name__ == "__main__":
    main()
