
import os
import tempfile
from pathlib import Path

from . import utils
from . import swissfel


def launch_indexing_job(*, list_file: Path, geometry_file: Path, output_stream_path: Path, config: swissfel.SwissFELConfig) -> int:

    with tempfile.TemporaryDirectory() as tempdir:
        script_path = os.path.join(tempdir, "indexing_sbatch.sh")

        script_content = f"""#!/bin/sh

module purge
module load crystfel/{config.crystfel_version}

indexamajig -i {list_file} \\
  --output={output_stream_path} \\
  --geometry={geometry_file} \\
  --pdb={config.cell_file_path} \\
  -j {config.number_of_cores} \\
  --peaks={config.peak_finding_method} \\
  --threshold={config.peak_threshold} \\
  --min-snr={config.min_snr} \\
  --min-pix-count={config.min_pixel_count} \\
  --min-res={config.min_resolution} \\
  --max-res={config.max_resolution} \\
  --indexing={config.indexing_method} \\
  
  --int-radius={config.integration_radius} \\
  --integration={config.integration_method} \\
  --local-bg-radius={config.local_bg_radius} \\
  --multi --retry --check-peaks
"""

        with open(script_path, "w") as f:
            f.write(script_content)

        job_id = utils.submit_job(script_path)

    return job_id
