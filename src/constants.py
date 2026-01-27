from pathlib import Path
import pandas as pd

BEAMLINE: str = "alvra"
EXPERIMENT_ID: str = "p21958"
DETECTOR_GEOMETRY_NAME: str = "JF06T08V07"

CELL = (83.22, 83.22, 89.97, 90, 90, 120)
INITIAL_GEOMETRY_FILE_PATH: Path = Path("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/JF06T08V07.geom")
CELL_FILE_PATH: Path = Path("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/X3S1.cell")

RUN_RANGE = (8, 125)


def geometry_file_for_run(run_number: int) -> Path:

    geometry_summary = pd.read_csv("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/geometry_summary.csv")
    optimized_base_path = Path(f"/das/work/p21/{EXPERIMENT_ID}/geometry-optimization")

    match = geometry_summary.loc[geometry_summary["run_number"] == run_number, "geometry_run"]
    if not match.empty:
        geometry_run_number = int(match.iloc[0])
    else:
        print(f"issue finding matching geometry for run {run_number}")

    geometry_file_path = optimized_base_path / Path(f"run{geometry_run_number:04d}/{geometry_run_number:04d}_optimized.geom")
    
    if not geometry_file_path.exists():
        raise IOError(f"file: {str(geometry_file_path)} not on disk!")

    return geometry_file_path


if __name__ == "__main__":
    geometry_summary = pd.read_csv("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/geometry_summary.csv")
    runs = list(geometry_summary["run_number"])
    for run in runs:
        print(f"{run} {geometry_file_for_run(run)}")
