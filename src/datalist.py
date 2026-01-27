
from pathlib import Path
from glob import glob
from .constants import BEAMLINE, EXPERIMENT_ID, DETECTOR_GEOMETRY_NAME


ALLOWED_LASER_STATES = ["light", "dark", "all"]


def get_list_files_for_run(run_number: int, laser_state: str = "all") -> list:

    if laser_state not in ALLOWED_LASER_STATES:
        raise RuntimeError()

    if laser_state == "all":
        laser_state = "*"

    glob_pattern = f"/sf/{BEAMLINE}/data/{EXPERIMENT_ID}/raw/run{run_number:04d}-*/data/acq????.{DETECTOR_GEOMETRY_NAME}.{laser_state}.lst"
    
    return glob(glob_pattern)


def get_combined_list_files_for_run(run_number: int, laser_state: str = "all") -> list:
    
    list_file_dir = Path("/das/work/p21/p21958/list-files")
    list_file_path = list_file_dir / Path(f"combined_run{run_number:04d}-{laser_state}.lst")
    
    with list_file_path.open("w") as combined_list_file:
        for list_file in get_list_files_for_run(run_number, laser_state=laser_state):
            with open(list_file) as infile:
                combined_list_file.write(infile.read())

    return list_file_path


def get_list_files_for_tag(tag_string: str, laser_state: str = "all") -> list:

    if laser_state not in ALLOWED_LASER_STATES:
        raise RuntimeError()

    if laser_state == "all":
        laser_state = "*"

    glob_pattern = f"/sf/{BEAMLINE}/data/{EXPERIMENT_ID}/raw/run????-{tag_string}/data/acq????.{DETECTOR_GEOMETRY_NAME}.{laser_state}.lst"
    
    return glob(glob_pattern)
