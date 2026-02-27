
from pathlib import Path

import yaml
from pydantic import BaseModel




class IndexingConfig(BaseModel):
    peak_finding_method: str
    peak_threshold: int
    min_snr: float
    min_pixel_count: int
    min_resolution: int
    max_resolution: int
    indexing_method: str
    integration_radius: str
    integration_method: str
    local_bg_radius: int



class GeometryOptimizationConfig(BaseModel):
    sample_size: int
    step_size: float
    clen_center: float
    clen_half_range: int
    run_range: tuple[int, int]


class MergingConfig(BaseModel):
    use_online_streams: bool
    symmetry: str
    partiality_model: str
    partialator_iterations: int
    pushres: float
    max_adu: int


class StatsConfig(BaseModel):
    stats_highres: float


class SwissFELConfig(BaseModel):
    beamline: str
    experiment_id: str
    detector_geometry_name: str
    crystfel_version: str
    allowed_laser_states: list[str]

    list_file_directory_path: Path
    initial_geometry_file_path: Path
    cell_file_path: Path

    geometry_summary_path: Path
    geometry_optimization_directory: Path

    stream_file_directory: Path
    merging_directory: Path
    mtz_directory: Path

    indexing: IndexingConfig
    geometry_optimization: GeometryOptimizationConfig
    merging: MergingConfig
    stats: StatsConfig

    @classmethod
    def from_yaml(cls, path: Path) -> "SwissFELConfig":
        with path.open("r") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)


def get_list_files_for_run(run_number: int, config: SwissFELConfig, laser_state: str = "all") -> list[Path]:

    if laser_state not in config.allowed_laser_states:
        raise RuntimeError()

    if laser_state == "all":
        laser_state = "*"

    glob_pattern = Path(f"/sf/{config.beamline}/data/{config.experiment_id}/raw/run{run_number:04d}-*/data/acq????.{config.detector_geometry_name}.{laser_state}.lst")

    return glob_pattern.glob()


def get_combined_list_files_for_run(run_number: int, config: SwissFELConfig, laser_state: str = "all") -> list:

    list_file_dir = config.list_file_directory_path
    list_file_path = list_file_dir / Path(f"combined_run{run_number:04d}-{laser_state}.lst")

    with list_file_path.open("w") as combined_list_file:
        for list_file in get_list_files_for_run(run_number, laser_state=laser_state):
            with open(list_file) as infile:
                combined_list_file.write(infile.read())

    return list_file_path


def get_list_files_for_tag(tag_string: str, config: SwissFELConfig, laser_state: str = "all") -> list:

    if laser_state not in config.allowed_laser_states:
        raise RuntimeError()

    if laser_state == "all":
        laser_state = "*"

    glob_pattern = Path(f"/sf/{config.beamline}/data/{config.experiment_id}/raw/run????-{tag_string}/data/acq????.{config.detector_geometry_name}.{laser_state}.lst")

    return glob_pattern.glob()
