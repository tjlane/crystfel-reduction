import pandas as pd
from pathlib import Path
from glob import glob


def load_stats_by_shell(stats_directory: str, tag: str) -> pd.DataFrame:

    check_stats_file = Path(stats_directory) / Path(f"{tag}_check.dat")
    rsplit_file      = Path(stats_directory) / Path(f"{tag}_rsplit.dat")
    cc_file          = Path(stats_directory) / Path(f"{tag}_cc.dat")
    ccstar_file      = Path(stats_directory) / Path(f"{tag}_ccstar.dat")

    for p in [check_stats_file, rsplit_file, cc_file, ccstar_file]:
        if not p.exists():
            raise IOError(f"cannot find {str(p)}")
        
    join_column = 'Center 1/nm'
    colspec = [(0,11), (11,24)]
        
    base_data = pd.read_fwf(check_stats_file, na_values=["-nan"])
    rsplit_data = pd.read_fwf(rsplit_file, na_values=["-nan"]).rename(columns={'1/d centre': join_column})
    cc_data = pd.read_fwf(cc_file, colspecs=colspec, na_values=["-nan"]).rename(columns={'1/d centr': join_column})
    ccstar_data = pd.read_fwf(ccstar_file, colspecs=colspec, na_values=["-nan"]).rename(columns={'1/d centr': join_column})

    base_data = base_data.merge(rsplit_data[[join_column, "Rsplit/%"]], on=join_column)
    base_data = base_data.merge(cc_data, on=join_column)
    base_data = base_data.merge(ccstar_data, on=join_column)

    return base_data


def count_number_of_crystals_merged(stream_file: Path) -> int:
    with stream_file.open("r") as f:
        count = sum(1 for line in f if line.startswith("Cell"))
    return count


def main():

    # right now just computes all
    base_path = Path("/das/work/p21/p21958")

    for dataset in glob(str(base_path / "final_merging/*")):
        basename = Path(dataset).name
        stats_path = Path(dataset) / "stats"
        for tag in [f"{basename}_light", f"{basename}_dark"]:
            if (stats_path / f"{tag}_check.dat").exists():

                laser_state = tag.split("_")[-1]
                n_indexed = count_number_of_crystals_merged(Path(dataset) / f"{basename}_combined_{laser_state}.stream")
                
                df = load_stats_by_shell(stats_path, tag)
                df.to_csv(base_path / "final_stats" / f"{tag}_stats_by_shell.csv", index=False)

                print(f"Processed {tag}\t{n_indexed}")


if __name__ == "__main__":
    main()

