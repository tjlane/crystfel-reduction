import pandas as pd

from crystred.geometry import geometry_file_for_run

geometry_summary = pd.read_csv("/das/home/ext-lane_t/opt/mmCPD-reduction/cpdred/swissfel/data/geometry_summary.csv")
runs = list(geometry_summary["run_number"])
for run in runs:
    print(f"{run} {geometry_file_for_run(run)}")