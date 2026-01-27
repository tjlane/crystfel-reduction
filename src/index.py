
import os
import tempfile

from . import utils


def launch_indexing_job(*, list_file, geometry_file, cell_file, output_stream_path):

    with tempfile.TemporaryDirectory() as tempdir:
        cryst_run_file = os.path.join(tempdir, f"indexing_sbatch.sh")

        with open(cryst_run_file, "w") as run_sh:
            run_sh.write("#!/bin/sh\n\n")
            run_sh.write("module purge\n")
            run_sh.write("module load crystfel/0.11.1\n")
            #
            run_sh.write(f"indexamajig -i {list_file} \\\n")
            run_sh.write(f"  --output={output_stream_path} \\\n")
            run_sh.write(f"  --geometry={geometry_file} \\\n")
            run_sh.write(f"  --pdb={cell_file} \\\n")
            run_sh.write("  -j 36 \\\n")
            run_sh.write("  --peaks=peakfinder8 \\\n")
            run_sh.write("  --threshold=50 \\\n")
            run_sh.write("  --min-snr=5 \\\n")
            run_sh.write("  --min-pix-count=2 \\\n")
            run_sh.write("  --min-res=85 \\\n")
            run_sh.write("  --max-res=3000 \\\n")
            run_sh.write("  --indexing=xgandalf-latt-cell \\\n")
            run_sh.write("  --multi --retry --check-peaks \\\n")
            run_sh.write("  --int-radius=2,3,6 \\\n")
            run_sh.write("  --integration=rings-grad \\\n")
            run_sh.write("  --local-bg-radius=4 \\\n")

        job_id = utils.submit_job(cryst_run_file)

    # return crystfel file name
    return job_id
