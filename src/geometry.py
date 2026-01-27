import pandas as pd
import os
from glob import glob
import tempfile
import regex as re
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from . import utils
from . import swissfel


def geometry_file_for_run(run_number: int, config: swissfel.SwissFELConfig) -> Path:

    geometry_summary = pd.read_csv(config.geometry_summary)
    optimized_base_path = config.geometry_optimization_directory

    match = geometry_summary.loc[geometry_summary["run_number"] == run_number, "geometry_run"]
    if not match.empty:
        geometry_run_number = int(match.iloc[0])
    else:
        print(f"issue finding matching geometry for run {run_number}")

    geometry_file_path = optimized_base_path / Path(f"run{geometry_run_number:04d}/{geometry_run_number:04d}_optimized.geom")
    
    if not geometry_file_path.exists():
        raise IOError(f"file: {str(geometry_file_path)} not on disk!")

    return geometry_file_path


def subsample_lst_file(lst_file_path: str, sample_size: int) -> str:
    # create sample of images from run
    # read h5.lst - note - removes // from image column
    cols = ["h5", "image"]
    sample_df = pd.read_csv(lst_file_path, sep="\s//", engine="python", names=cols)

    # take defined sample
    if len(sample_df) > sample_size:
        sample_df = sample_df.sample(sample_size)

    # sort list
    sample_df = sample_df.sort_index()

    # re-add // to image columm
    sample_df["image"] = "//" + sample_df.image.astype(str)

    # write sample to file
    sample_file = "h5_{0}_sample.lst".format(sample_size)
    sample_df.to_csv(sample_file, sep=" ", index=False, header=False)

    # return sample file name
    return sample_file


def change_geometry_clen(input_geom_file: Path, clen: float) -> Path:

    with open(input_geom_file, "r") as initial_geometry:
        clen_geom = re.sub("clen = 0\.\d+", "clen = {0}".format(clen), initial_geometry.read())

    # write new clen_geom to file
    clen_geom_file = f"{clen:.5f}.geom"
    with open(clen_geom_file, "w") as geom:
        geom.write(clen_geom)

    # return clen_geom file name
    return Path(clen_geom_file)


def launch_indexing_job_for_geom_optimization(
        clen: float,
        list_file: Path,
        initial_geom_file: Path,
        cell_file: Path
    ) -> int:

    clen_geom_file = change_geometry_clen(initial_geom_file, clen)

    with tempfile.TemporaryDirectory() as tempdir:
        cryst_run_file = os.path.join(tempdir, f"{clen}_run.sh")

        with open(cryst_run_file, "w") as run_sh:
            run_sh.write("#!/bin/sh\n\n")
            run_sh.write("module purge\n")
            run_sh.write("module load crystfel/0.10.2\n")
            run_sh.write(f"indexamajig -i {list_file} \\\n")
            run_sh.write(f"  --output={clen:.5f}.stream \\\n")
            run_sh.write(f"  --geometry={str(clen_geom_file)}\\\n")
            run_sh.write(f"  --pdb={str(cell_file)} \\\n")
            run_sh.write("  --indexing=mosflm-latt-cell --peaks=peakfinder8 \\\n")
            run_sh.write("  --threshold=50 --min-snr=5 --local-bg-radius=4 --int-radius=4,5,6 --tolerance=10.0,10.0,10.0,3,3,3\\\n")
            run_sh.write(
                "  -j 36 --no-multi --no-retry --max-res=3000 --min-pix-count=2 --min-res=85\n\n"
            )

        job_id: int = utils.submit_job(cryst_run_file)

    return job_id


def scan_for_optimal_geometry(
    *,
    working_dir: Path, 
    list_file: Path,
    initial_geom_file: Path,
    cell_file: Path, 
    clens_to_scan : list[float],
    subsample_size: int = 5000,
):

    # make sample list
    sample_h5 = subsample_lst_file(list_file, subsample_size)
    subsampled_list_file = os.path.abspath(sample_h5)

    # submitted job set and job_list
    submitted_job_ids = set()

    # make directorys for results
    print("begin CrystFEL anaylsis of different clens")

    # loop to cycle through clen steps
    for clen in clens_to_scan:
        print(f"testing clen = {clen:.5f}")
        # define process directory
        proc_dir = Path(working_dir) / f"{clen:.5f}"

        # make process directory
        if not os.path.exists(proc_dir):
            os.makedirs(proc_dir)

        # move to process directory
        os.chdir(proc_dir)

        # make crystfel run file
        job_id = launch_indexing_job_for_geom_optimization(
            clen,
            subsampled_list_file,
            initial_geom_file,
            cell_file
        )
        submitted_job_ids.add(job_id)

        # move back to cwd
        os.chdir(working_dir)

    # wait for jobs to complete
    utils.wait_for_jobs(submitted_job_ids)
    print("slurm processing done")


# -----------------------------------------------------------------------------


def scrub_clen(stream_pwd):
    pattern = r"0\.\d+/(0\.\d+)\.stream"
    re_search = re.search(pattern, stream_pwd)
    clen = re_search.group(1)
    return float(clen)


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


def determine_statistic_minimum(cell_dataframe, staistic_name, polyfit_degree=2, r2tol=0.1):

    x = cell_dataframe['clen'].values
    y = cell_dataframe[staistic_name].values
    
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
            "skew_a" : cells_df.a.skew(),
            "skew_b" : cells_df.b.skew(),
            "skew_c" : cells_df.c.skew(),
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

    # std_a plot
    color = "lightsteelblue"
    ax2.plot(stats_df.clen, stats_df.std_a, 'o', color=color)

    # std_b plot
    color = "cornflowerblue"
    ax2.plot(stats_df.clen, stats_df.std_b, 'o', color=color)

    # std_c plot
    color = "royalblue"
    ax2.plot(stats_df.clen, stats_df.std_c, 'o', color=color)


def plot_indexed_std_alpha_beta_gamma(stats_df, ax1, ax2):
    # indexed images plot
    color = "tab:red"
    ax1.set_xlabel("clen")
    ax1.set_ylabel("indexed", color=color)
    ax1.plot(stats_df.clen, stats_df.indexed, 'o', color=color)
    ax1.tick_params(axis="y", labelcolor=color)

    # label color
    color = "tab:green"
    ax2.set_ylabel("alpha, beta, gamma st.deviation", color=color)
    ax2.tick_params(axis="y", labelcolor=color)

    # std_alpha plot
    color = "limegreen"
    ax2.plot(stats_df.clen, stats_df.std_alpha, 'o', color=color)

    # std_beta plot
    color = "darkgreen"
    ax2.plot(stats_df.clen, stats_df.std_beta, 'o', color=color)

    # std_gamma plot
    color = "green"
    ax2.plot(stats_df.clen, stats_df.std_gamma, 'o', color=color)


# -----------------------------------------------------------------------------


def detector_shift(initial_geometry_path: Path, stream_file_paths: list[Path]) -> None:
    # generates a new file "-predrefine.geom"

    # Determine the mean shifts
    x_shifts = []
    y_shifts = []
    z_shifts = []

    prog1 = re.compile("^predict_refine/det_shift\sx\s=\s([0-9\.\-]+)\sy\s=\s([0-9\.\-]+)\smm$")
    prog2 = re.compile("^predict_refine/clen_shift\s=\s([0-9\.\-]+)\smm$")

    for file in stream_file_paths:

        f = open(file, 'r')

        while True:

            fline = f.readline()
            if not fline:
                break

            match = prog1.match(fline)
            if match:
                xshift = float(match.group(1))
                yshift = float(match.group(2))
                x_shifts.append(xshift)
                y_shifts.append(yshift)

            match = prog2.match(fline)
            if match:
                zshift = float(match.group(1))
                z_shifts.append(zshift)

        f.close()

    mean_x = sum(x_shifts) / len(x_shifts)
    mean_y = sum(y_shifts) / len(y_shifts)

    with open("detector-shift.log", "w") as f:
        f.write('Mean shifts: dx = {:.2} mm,  dy = {:.2} mm'.format(mean_x,mean_y))

    out = os.path.splitext(initial_geometry_path)[0]+'-predrefine.geom'
    g = open(initial_geometry_path, 'r')
    h = open(out, 'w')
    panel_resolutions = {}

    prog1 = re.compile("^\s*res\s+=\s+([0-9\.]+)\s")
    prog2 = re.compile("^\s*(.*)\/res\s+=\s+([0-9\.]+)\s")
    prog3 = re.compile("^\s*(.*)\/corner_x\s+=\s+([0-9\.\-]+)\s")
    prog4 = re.compile("^\s*(.*)\/corner_y\s+=\s+([0-9\.\-]+)\s")
    default_res = 0
    while True:

        fline = g.readline()
        if not fline:
            break

        match = prog1.match(fline)
        if match:
            default_res = float(match.group(1))
            h.write(fline)
            continue

        match = prog2.match(fline)
        if match:
            panel = match.group(1)
            panel_res = float(match.group(2))
            default_res =  panel_res
            panel_resolutions[panel] = panel_res
            h.write(fline)
            continue

        match = prog3.match(fline)
        if match:
            panel = match.group(1)
            panel_cnx = float(match.group(2))
            if panel in panel_resolutions:
                res = panel_resolutions[panel]
            else:
                res = default_res
            h.write('%s/corner_x = %f\n' % (panel,panel_cnx+(mean_x*res*1e-3)))
            continue

        match = prog4.match(fline)
        if match:
            panel = match.group(1)
            panel_cny = float(match.group(2))
            if panel in panel_resolutions:
                res = panel_resolutions[panel]
            else:
                res = default_res
            h.write('%s/corner_y = %f\n' % (panel,panel_cny+(mean_y*res*1e-3)))
            continue

        h.write(fline)

    g.close()
    h.close()
