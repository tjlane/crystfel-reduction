import pandas as pd
import os
from glob import glob
import regex as re
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from . import utils
from . import config
from . import index


def geometry_file_for_run(run_number: int, cfg: config.SwissFELConfig) -> Path:

    geometry_summary = pd.read_csv(cfg.geometry_summary_path)
    optimized_base_path = cfg.geometry_optimization_directory

    match = geometry_summary.loc[geometry_summary["run_number"] == run_number, "geometry_run"]
    if match.empty:
        raise ValueError(f"No matching geometry found for run {run_number}")

    geometry_run_number = int(match.iloc[0])
    geometry_file_path = optimized_base_path / f"run{geometry_run_number:04d}/{geometry_run_number:04d}_optimized.geom"

    if not geometry_file_path.exists():
        raise IOError(f"file: {str(geometry_file_path)} not on disk!")

    return geometry_file_path


def subsample_lst_file(lst_file_path: Path, sample_size: int) -> Path:
    # create sample of images from run
    # read h5.lst - note - removes // from image column
    cols = ["h5", "image"]
    sample_df = pd.read_csv(lst_file_path, sep=r"\s//", engine="python", names=cols)

    # take defined sample
    if len(sample_df) > sample_size:
        sample_df = sample_df.sample(sample_size)

    # sort list
    sample_df = sample_df.sort_index()

    # re-add // to image column
    sample_df["image"] = "//" + sample_df.image.astype(str)

    # write sample to file
    sample_file = lst_file_path.parent / f"h5_{sample_size}_sample.lst"
    sample_df.to_csv(sample_file, sep=" ", index=False, header=False)

    return sample_file


def change_geometry_clen(input_geom_file: Path, clen: float, output_dir: Path) -> Path:

    with open(input_geom_file, "r") as initial_geometry:
        clen_geom = re.sub(r"clen = \d+\.?\d*", f"clen = {clen}", initial_geometry.read())

    clen_geom_file = output_dir / f"{clen:.5f}.geom"
    with open(clen_geom_file, "w") as geom:
        geom.write(clen_geom)

    return clen_geom_file



def scan_for_optimal_geometry(
    *,
    working_dir: Path,
    list_file: Path,
    initial_geom_file: Path,
    cfg: config.SwissFELConfig,
    clens_to_scan: list[float],
    subsample_size: int = 5000,
):
    working_dir = Path(working_dir)

    # make sample list
    sample_list_file = subsample_lst_file(list_file, subsample_size)

    submitted_job_ids = set()

    print("begin CrystFEL analysis of different clens")

    for clen in clens_to_scan:
        print(f"testing clen = {clen:.5f}")

        proc_dir = working_dir / f"{clen:.5f}"
        proc_dir.mkdir(parents=True, exist_ok=True)

        clen_geom_file = change_geometry_clen(initial_geom_file, clen, proc_dir)
        job_id = index.launch_indexing_job(
            list_file=sample_list_file,
            geometry_file=clen_geom_file,
            output_stream_path=proc_dir / f"{clen:.5f}.stream",
            config=cfg,
        )
        submitted_job_ids.add(job_id)

    utils.wait_for_jobs(submitted_job_ids)
    print("slurm processing done")


# -----------------------------------------------------------------------------


def scrub_clen(stream_path: str) -> float:
    pattern = r"[\d.]+/([\d.]+)\.stream"
    match = re.search(pattern, stream_path)
    if match is None:
        raise ValueError(f"Could not parse clen from path: {stream_path}")
    return float(match.group(1))


def stream_to_unitcell_dataframe(stream_file_path, max_num_cells=None):
    pattern = re.compile(
        r"Cell\sparameters\s(\d+\.\d+)\s(\d+\.\d+)\s(\d+\.\d+)\snm,\s"
        r"(\d+\.\d+)\s(\d+\.\d+)\s(\d+\.\d+)\sdeg"
    )

    data = []

    with open(stream_file_path, "r") as stream_f:
        for line in stream_f:
            match = pattern.search(line)
            if match:
                data.append([float(val) for val in match.groups()])

            if max_num_cells and (len(data) >= max_num_cells):
                break

    cols = ["a", "b", "c", "alpha", "beta", "gamma"]
    return pd.DataFrame(data, columns=cols)


def determine_statistic_minimum(cell_dataframe, statistic_name, polyfit_degree=2, r2tol=0.1):

    x = cell_dataframe['clen'].values
    y = cell_dataframe[statistic_name].values

    coefs = np.polyfit(x, y, polyfit_degree)
    p = np.poly1d(coefs)

    y_pred = p(x)

    y_mean = np.mean(y)
    ss_tot = np.sum((y - y_mean)**2)
    ss_res = np.sum((y - y_pred)**2)
    r2 = 1 - (ss_res / ss_tot)

    if not r2 > r2tol:
        print(f"R^2 worryingly low: {r2}")

    clen_at_stat_min = x[np.argmin(y_pred)]

    return clen_at_stat_min


def determine_clen_from_scan(scan_top_dir, plot=False, stat_to_optimize="std_c"):

    # from preliminary tests, parameters a, b, gamma appear reliable

    stats_df = compute_unitcell_statistics_as_function_of_clen(scan_top_dir)
    stats_df.to_csv("lattice_stats_summary.csv")

    suggested_clen = determine_statistic_minimum(stats_df, stat_to_optimize)

    if plot:
        fig, (ax1, ax3) = plt.subplots(1, 2)
        ax2 = ax1.twinx()
        ax4 = ax3.twinx()

        plot_indexed_std(stats_df, ax1, ax2)
        plot_indexed_std_alpha_beta_gamma(stats_df, ax3, ax4)

        fig.tight_layout()
        plt.savefig("clen_opt.png")

    print(f"Determined clen: {suggested_clen}")

    return suggested_clen


def compute_unitcell_statistics_as_function_of_clen(scan_top_dir):

    stats: list[dict] = []

    glob_pattern = os.path.join(scan_top_dir, "*/*.stream")
    for stream_file_path in glob(glob_pattern):

        clen = scrub_clen(stream_file_path)
        cells_df = stream_to_unitcell_dataframe(stream_file_path)
        print(f"analyzing clen = {clen} / {len(cells_df)} indexed")

        stats.append({
            "clen": clen,
            "indexed": len(cells_df),
            "std_a": cells_df.a.std(),
            "std_b": cells_df.b.std(),
            "std_c": cells_df.c.std(),
            "std_alpha": cells_df.alpha.std(),
            "std_beta": cells_df.beta.std(),
            "std_gamma": cells_df.gamma.std(),
            "skew_a": cells_df.a.skew(),
            "skew_b": cells_df.b.skew(),
            "skew_c": cells_df.c.skew(),
        })

    stats_df = pd.DataFrame(stats)

    return stats_df


def plot_indexed_std(stats_df, ax1, ax2):
    # indexed images plot
    color = "tab:red"
    ax1.set_xlabel("clen")
    ax1.set_ylabel("indexed", color=color)
    ax1.plot(stats_df.clen, stats_df.indexed, 'o', color=color)
    ax1.tick_params(axis="y", labelcolor=color)

    # label color
    color = "tab:blue"
    ax2.set_ylabel("a,b,c st.deviation", color=color)
    ax2.tick_params(axis="y", labelcolor=color)

    ax2.plot(stats_df.clen, stats_df.std_a, 'o', color="lightsteelblue")
    ax2.plot(stats_df.clen, stats_df.std_b, 'o', color="cornflowerblue")
    ax2.plot(stats_df.clen, stats_df.std_c, 'o', color="royalblue")


def plot_indexed_std_alpha_beta_gamma(stats_df, ax1, ax2):
    # indexed images plot
    color = "tab:red"
    ax1.set_xlabel("clen")
    ax1.set_ylabel("indexed", color=color)
    ax1.plot(stats_df.clen, stats_df.indexed, 'o', color=color)
    ax1.tick_params(axis="y", labelcolor=color)

    color = "tab:green"
    ax2.set_ylabel("alpha, beta, gamma st.deviation", color=color)
    ax2.tick_params(axis="y", labelcolor=color)

    ax2.plot(stats_df.clen, stats_df.std_alpha, 'o', color="limegreen")
    ax2.plot(stats_df.clen, stats_df.std_beta, 'o', color="darkgreen")
    ax2.plot(stats_df.clen, stats_df.std_gamma, 'o', color="green")


# -----------------------------------------------------------------------------


def detector_shift(initial_geometry_path: Path, stream_file_paths: list[Path]) -> None:
    # generates a new file "-predrefine.geom"

    x_shifts = []
    y_shifts = []

    prog_det = re.compile(r"^predict_refine/det_shift\sx\s=\s([0-9.\-]+)\sy\s=\s([0-9.\-]+)\smm$")

    for file in stream_file_paths:
        with open(file, 'r') as f:
            for fline in f:
                match = prog_det.match(fline)
                if match:
                    x_shifts.append(float(match.group(1)))
                    y_shifts.append(float(match.group(2)))

    if not x_shifts or not y_shifts:
        raise ValueError("No predict_refine/det_shift entries found in stream files")

    mean_x = sum(x_shifts) / len(x_shifts)
    mean_y = sum(y_shifts) / len(y_shifts)

    with open("detector-shift.log", "w") as f:
        f.write('Mean shifts: dx = {:.2f} mm,  dy = {:.2f} mm'.format(mean_x, mean_y))

    out = initial_geometry_path.with_name(initial_geometry_path.stem + '-predrefine.geom')

    prog1 = re.compile(r"^\s*res\s+=\s+([0-9.]+)\s")
    prog2 = re.compile(r"^\s*(.*)/res\s+=\s+([0-9.]+)\s")
    prog3 = re.compile(r"^\s*(.*)/corner_x\s+=\s+([0-9.\-]+)\s")
    prog4 = re.compile(r"^\s*(.*)/corner_y\s+=\s+([0-9.\-]+)\s")

    panel_resolutions = {}
    default_res = 0

    with open(initial_geometry_path, 'r') as g, open(out, 'w') as h:
        for fline in g:

            match = prog1.match(fline)
            if match:
                default_res = float(match.group(1))
                h.write(fline)
                continue

            match = prog2.match(fline)
            if match:
                panel = match.group(1)
                panel_res = float(match.group(2))
                default_res = panel_res
                panel_resolutions[panel] = panel_res
                h.write(fline)
                continue

            match = prog3.match(fline)
            if match:
                panel = match.group(1)
                panel_cnx = float(match.group(2))
                res = panel_resolutions.get(panel, default_res)
                h.write('%s/corner_x = %f\n' % (panel, panel_cnx + (mean_x * res * 1e-3)))
                continue

            match = prog4.match(fline)
            if match:
                panel = match.group(1)
                panel_cny = float(match.group(2))
                res = panel_resolutions.get(panel, default_res)
                h.write('%s/corner_y = %f\n' % (panel, panel_cny + (mean_y * res * 1e-3)))
                continue

            h.write(fline)
